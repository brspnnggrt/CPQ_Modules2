import clr

clr.AddReference("System.Xml")

import System
from System.Xml import XmlDocument, XmlNamespaceManager

from Helper import Helper, CPQ
from Mappings import Mappings
from CustomException import CustomException


class WebServiceRequestBuilder:

    @staticmethod
    def ProcessMessage(service, dictionary, parent=None):
        '''
            Recursive method to convert a python dictionary into a .NET object using reflection
            The parent argument is only used for recursion
            Example call:

            > WebServiceRequestBuilder.ProcessMessage(service, {"query": {"property -> propertytype": "value" }})

            -  where "query" corresponds to a class in the service assembly
            -  where "property" corresponds to a property of the class corresponding to "query"
            -  where "propertytype" corresponds to a class in the service assembly used by the property corresponding to the related "property"
            -  where "value" is either a python dictionary, a list of python dictionaries, or a value (ie. string, number or .NET type)

            if property and propertytype are equal, you can omit the propertytype as follows:

            > WebServiceRequestBuilder.ProcessMessage(service, {"query": {"property": "value" }})

            if the value is another python dictionary, it should be in the same format as described here
        '''

        newObject = None

        # Simply process each key value pair in the dictionary
        for key, value in dictionary.items():

            if type(value) is dict:  # value is another python dictionary -> handle recursively

                # Create new object for property
                newObject = WebServiceRequestBuilder.New(service, key)

                # Process value recursively
                WebServiceRequestBuilder.ProcessMessage(service, value, newObject)

                # Set field on parent if parent exists
                if parent is not None:
                    WebServiceRequestBuilder._ProcessField(parent, key, newObject)

            else:  # Value is not a python dictionary

                # Value is a list of python dictionaries
                if type(value) is list and len(value) > 0 and type(value[0]) is dict:

                    # Handle recursively for each item in list using a map function
                    processedList = [WebServiceRequestBuilder.ProcessMessage(service, property) for property in value]

                    # Convert list to generic .NET type and set field on parent
                    WebServiceRequestBuilder._ProcessField(parent, key, WebServiceRequestBuilder._ConvertListToGenericArray(processedList))
                    continue

                # Value is a list
                elif type(value) is list:

                    # Convert list to generic .NET type and set field on parent
                    WebServiceRequestBuilder._ProcessField(parent, key, WebServiceRequestBuilder._ConvertListToGenericArray(value))
                    continue

                # Value is not a list and not a dict -> simply set field on parent
                WebServiceRequestBuilder._ProcessField(parent, key, value)

        return newObject

    @staticmethod
    def New(service, typeName, **kwargs):
        ''' Create new instance using reflection, **this is necessary**(!) because of namespace collisions in generated C4C web service assemblies'''

        if "->" in typeName:
            typeName = typeName.split("->")[1].strip()

        # create instance
        assembly = clr.GetClrType(type(service)).Assembly
        try:
            instanceType = assembly.GetTypes().First(lambda x: x.Name == typeName)
        except SystemError:
            raise CustomException("Type '{0}' does not exist in this assembly".format(typeName))
        from System import Activator
        instance = Activator.CreateInstance(instanceType)

        # set provided kwargs as fields on the newly created object
        if kwargs is not None:
            for key, value in kwargs.iteritems():
                field = instanceType.GetField(key)
                field.SetValue(instance, value)

        return instance

    @staticmethod
    def _ProcessField(instance, field, value):
        ''' Process field to set the value supporting '->' syntax '''

        # Get actual fieldname (without type)
        if "->" in field:
            field = field.split("->")[0].strip()

        WebServiceRequestBuilder._SetField(instance, field, value, True)

        # Set field 'Specified' if exists
        field = field + 'Specified'

        WebServiceRequestBuilder._SetField(instance, field, True, False)

    @staticmethod
    def _SetField(instance, field, value, strict):
        '''Use reflection to set a field value or property value on an object'''

        # Get Field
        instanceType = instance.GetType()
        fieldInfo = instanceType.GetField(field)  # try to get as field
        if fieldInfo is None:
            fieldInfo = instanceType.GetProperty(field)  # fallback on property
            if fieldInfo is None and strict:
                typeName = type(instance).__name__
                raise CustomException("Could not get fieldInfo for instance of type '{0}' with field '{1}'".format(typeName, field))
            elif fieldInfo is None:
                return

        # Set field
        try:
            fieldInfo.SetValue(instance, value)
        except ValueError as e:
            typeName = type(instance).__name__
            raise CustomException("Could not set field '{0}' for instance of type '{1}' - innerexception: {2} ".format(field, typeName, e.message))

        return

    @staticmethod
    def _ConvertListToGenericArray(list):
        '''Convert python list to a generic array in .NET with the correct type'''

        if len(list) > 0:
            genericType = list[0].GetType()
            return System.Array[genericType](list)

        return None

    @staticmethod
    def GetService(key, username, password, SAPID):
        '''
            Generate .NET assembly based on wsdl description that is found through a custom table lookup with the provided key.
            If the URL does not work, the correct URL will be retrieved from the wsil description (/sap/ap/srt/wsil) in C4S.
        '''
        query = "SELECT * FROM {0} WHERE name = '{1}'".format(Mappings.CustomTables.WebServices, key)
        record = SqlHelper.GetFirst(query)
        try:

            # Return service class from generated assembly
            return CPQ.WebServiceHelper.Load('wsdl', record.wsdl, username, password)
        except:  # noqa E722

            Log.Write("Could not load webservice, fetching wsdl location from wsil")

            wsilXmlString = WebServiceRequestBuilder._GetWsil(username, password, SAPID)  # Get wsil
            wsdl = WebServiceRequestBuilder._GetWsdlLocation(wsilXmlString, key)  # Get wsdl location from wsil

            Log.Write("wsil xml retrieved, saving wsdl location and retrying loading service with fresh URL")

            # Save new location in custom table
            tableInfo = SqlHelper.GetTable(Mappings.CustomTables.WebServices)
            tablerow = {"CpqTableEntryId": record.CpqTableEntryId, "name": key, "wsdl": wsdl}
            tableInfo.AddRow(tablerow)
            SqlHelper.Upsert(tableInfo)

            # Return service class from generated assembly
            return CPQ.WebServiceHelper.Load('wsdl', wsdl, username, password)

    @staticmethod
    def _GetWsil(username, password, SAPID):
        '''Send request to C4C to get full description of available web service descriptions (wsdl)'''

        # Create encoded credentials object
        credentialsEncoded = Helper.Python.EncodeCredentialsForBasicAuthentication(username, password)

        # Send request with credentials
        url = "https://my{0}.crm.ondemand.com/sap/ap/srt/wsil".format(SAPID)
        response = Helper.Python.HttpGet(url, credentialsEncoded)

        # Process stream to read out XML as string
        stream = CPQ.StreamReader(response.GetResponseStream())
        fullXmlString = stream.ReadToEnd()
        stream.Close()

        return fullXmlString

    @staticmethod
    def _GetWsdlLocation(wsilXmlString, key):
        '''Parse location of wsdl from the wsil xml string, the key should correspond to the value in the wsil xml document'''

        xmlDoc = XmlDocument()
        xmlDoc.LoadXml(wsilXmlString)

        manager = XmlNamespaceManager(xmlDoc.NameTable)
        manager.AddNamespace("wsil", "http://schemas.xmlsoap.org/ws/2001/10/inspection/")

        # example key: Query Sales Quotes
        condition = 'wsil:abstract[contains(text(),"objname={0}")]'.format(key)
        xpathQuery = '/wsil:inspection/wsil:service[{0}]/wsil:description/@location'.format(condition)

        locationAttribute = xmlDoc.SelectSingleNode(xpathQuery, manager)
        return locationAttribute.Value
