import clr
clr.AddReference('System.Web')

import System
from System import Uri
from System.Net import WebClient
from System.Web import HttpUtility
from System.Threading.Tasks import Task

from Helper import Helper
from Objects import Objects

class OdataRequest:
    ''' individual request model '''

    class Method:
        ''' Available methods for odata requests'''
        GET = "GET"
        POST = "POST"
        PATCH = "PATCH"
        PUT = "PUT"
        DELETE = "DELETE"

    def __init__(self, method, url, query={}, contentId=None, body=None, contentType="application/json", accept="application/json"):
        ''' create new request '''
        self.method = method
        self.url = url
        self.query = query
        self.contentId = contentId
        self.body = body
        self.contentType = contentType
        self.accept = accept

    def getUrl(self):
        return str(self).split(" ")[1]

    def __str__(self):
        ''' create string respresentation of individual request '''

        # default to json format for get queries
        if self.method == OdataRequest.Method.GET and "format" not in self.query.keys():
            self.query["format"] = "json"

        # request
        requestUrl = self.url
        for key in self.query.keys():
            if self.query.keys()[0] == key:  # is first
                requestUrl += "?"
            requestUrl += "${key}={value}".format(key=key, value=HttpUtility.UrlEncode(self.query[key]))
            if self.query.keys()[-1] != key:  # not last
                requestUrl += "&"
        request = " ".join([self.method, requestUrl, "HTTP/1.1"])

        # headers
        if self.method != OdataRequest.Method.GET:
            request += "\nContent-Type: " + self.contentType
        if self.contentId is not None:
            request += "\nContent-ID: " + self.contentId
        if self.body is not None:
            request += "\nContent-Length: " + str(len(self.body))
        if self.method != OdataRequest.Method.GET:
            request += "\nAccept: " + self.accept

        # body
        if self.body is not None:
            request += "\n\n" + self.body
        else:
            request += "\n"
        return request

class OdataChangeset:
    ''' array of requests that make changes (POST, PATCH, ...) '''

    def __init__(self, changeRequests):
        ''' Create new changeset '''
        self.changeRequests = changeRequests

    def __str__(self):
        ''' create string representation of changeset request '''

        fullRequest, boundary = Odata._combineRequests("changeset", self.changeRequests)
        rawChangeset = "Content-Type: multipart/mixed; boundary={0}\n\n".format(boundary)
        rawChangeset += fullRequest

        return rawChangeset

class Odata:
    ''' Call C4C web services (for easy debugging: use fiddler) '''

    def __init__(self, config):
        ''' Create new Odata webservice helper '''

        self.username = config.environment.OdataServiceUsername
        self.password = config.environment.OdataServicePassword
        self.sapId = config.environment.SAPID

        self.odataCredentials = Helper.Python.EncodeCredentialsForBasicAuthentication(self.username, self.password)

        self.csrf = self._getCsrf()
        self.logging = config.environment.WebServiceTrafficLogging

        self.isAsync = config.environment.OdataMethod.upper() == "Async".upper()

    def _GetExecutor(self, contentType, executionType, methodType, acceptType=None):

        client = WebClient()
        client.Headers.Add("Authorization", "Basic " + self.odataCredentials)
        client.Headers.Add("x-csrf-token", self.csrf.token)
        client.Headers.Add("Cookie", ";".join(self.csrf.cookies))
        client.Headers.Add("Content-Type", contentType)
        if acceptType is not None:
            client.Headers.Add("Accept", acceptType)

        actions = {
            "Sync": {
                "Download": client.DownloadString,
                "Upload": client.UploadString
            },
            "Async": {
                "Download": client.DownloadStringTaskAsync,
                "Upload": client.UploadStringTaskAsync
            }
        }

        return actions[executionType][methodType]

    def _GetExecutionType(self):
        return "Async" if self.isAsync else "Sync"

    def _GetMethodType(self, method):
        return "Download" if method == "GET" else "Upload"

    def _ExecuteRaw(self, action, url, body, method):

        if self.logging:
            Log.Write("--ODATA REQUEST--\n\n" + str(url) + "\n\n" + str(body))

        args = [Uri(url)] if self._GetMethodType(method) == "Download" else [Uri(url), method, body]

        response, duration = Helper.Utility.ExecuteAndTimeAction(action, *args)

        if self.logging:
            Log.Write("--ODATA RESPONSE [{0}s]--\n\n{1}".format(str(duration), response))

        return response

    def Execute(self, request):

        baseUrl="https://my{0}.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/{1}"

        executionType = self._GetExecutionType()
        methodType = self._GetMethodType(request.method)
        rawRequest = request.getUrl()
        url = baseUrl.format(self.sapId, rawRequest)

        action = self._GetExecutor(request.contentType, executionType, methodType, request.accept)

        response = self._ExecuteRaw(action, url, request.body, request.method)

        return Odata._parseJson(response) if self.isAsync is False else (response, Odata._parseJson)

    def ExecuteBatch(self, requests):
        ''' Execute requests in batch '''

        for changeSet in requests:
            if isinstance(changeSet, OdataChangeset):
                for i, req in enumerate(changeSet.changeRequests):
                    if req.contentId is None:
                        req.contentId = str(i)

        fullRequest, boundary = Odata._combineRequests("batch", requests)

        contentType = "multipart/mixed; boundary=" + boundary
        executionType = self._GetExecutionType()
        methodType = self._GetMethodType("POST")
        action = self._GetExecutor(contentType, executionType, methodType)

        baseUrl = "https://my{0}.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/$batch"
        url = baseUrl.format(self.sapId)
        response = self._ExecuteRaw(action, url, fullRequest, "POST")

        return self._parseBatchResponse(response) if self.isAsync is False else (response, Odata._parseBatchResponse)

    def _getCsrf(self):
        ''' Get csrf token from c4c '''

        Log.Write("reloading csrf token")

        client = WebClient()
        client.Headers.Add("Authorization", "Basic " + self.odataCredentials)
        client.Headers.Add("x-csrf-token", "fetch")
        client.Headers.Add("Content-Type", "application/json")
        result = client.DownloadString("https://my{0}.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/$metadata".format(self.sapId))

        csrf = Objects.Dynamic()
        csrf.token = client.ResponseHeaders["x-csrf-token"]
        csrf.cookies = client.ResponseHeaders["set-cookie"].split(",")
        return csrf

    @staticmethod
    def _combineRequests(type, requests):
        ''' generate boundary and use it to create a odata compatible string representation for batching '''

        def format(req):

            template = 'Content-Type: application/http\nContent-Transfer-Encoding:binary\n\n{0}\n\n'  # \n are important!!

            if isinstance(req, OdataChangeset):
                return "{0}\n".format(req)
            else:
                return template.format(req)

        subRequests = [format(req) for req in requests]  # add http request headers to subrequests

        boundary = type + "_" + str(System.Guid.NewGuid())  # seperator is a random guid
        fullSeparator = "--" + boundary  # seperator in correct format

        result = fullSeparator + "\n"
        for subRequest in subRequests:
            result += subRequest
            if subRequests[-1] != subRequest:  # not last
                result += "{0}\n".format(fullSeparator)  # separate subrequests using fullSepeartor
            else:  # last item
                result += "{0}--".format(fullSeparator)  # separate subrequests using fullSepeartor

        return (result, boundary)

    @staticmethod
    def _parseBatchResponse(batchResponse):
        ''' parse the response (string) into a python object '''

        batchResponse = batchResponse.replace("\r\n", "\n").replace("\r", "\n")  # remove incompatible newlines
        batchBoundary = batchResponse[0:batchResponse.index("\n")]  # first line contains boundary

        # split on boundary - remove empty elements - remove element if '--\n' (=last element)
        batchResponses = filter(lambda x: x != "" and x != "--\n", batchResponse.split(batchBoundary))

        return [Odata._processResponse(rawResponse) for rawResponse in batchResponses]

    @staticmethod
    def _getResponseObject(rawResponse):
        rawSplit = rawResponse.split("\n\n")[1:]  # split by 2 newlines - ignore first part (=batch information)

        rawResponseHeaders = rawSplit[0].split("\n")
        response = Objects.Dynamic()
        response.StatusLine = rawResponseHeaders.pop(0)  # retrieve & remove first line, leaving only headers

        def _getHeader(rawHeader):
            rawHeaderSplit = rawHeader.split(": ")
            header = Objects.Dynamic()
            header.Name = rawHeaderSplit[0]
            header.Value = rawHeaderSplit[1]
            return header

        response.Headers = [_getHeader(rawHeader) for rawHeader in rawResponseHeaders]
        response.Body = Odata._parseJson(rawSplit[1])

        return response

    @staticmethod
    def _parseJson(rawJsonString):
        json = RestClient.DeserializeJson(rawJsonString)
        if hasattr(json, "d"):
            json = json.d
        if hasattr(json, "results"):
            json = json.results
        return json

    @staticmethod
    def _getChangesetObject(rawResponse):
        firstLine = rawResponse[0:rawResponse.index("\n")]  # first line contains boundary
        changesetBoundary = "--" + firstLine.split("; ")[1].split("=")[1]  # ex: Content-Type: multipart/mixed; boundary=ejjeeffe1

        # split on boundary - remove first & last element
        changesetResponses = rawResponse.split(changesetBoundary)[1:-1]

        return [Odata._processResponse(rawChangesetResponse) for rawChangesetResponse in changesetResponses]

    @staticmethod
    def _processResponse(rawResponse):
        rawResponse = rawResponse.lstrip()
        if rawResponse.startswith("Content-Type: application/http"):
            return Odata._getResponseObject(rawResponse)
        if rawResponse.startswith("Content-Type: multipart/mixed; boundary="):
            return Odata._getChangesetObject(rawResponse)
        if rawResponse.startswith("Content-Type: multipart/mixed"):
            return None

    @staticmethod
    def Await(task, transformations=[]):

        # GetResult will wait for the result of the http request, and return it
        resultAfterWaiting = task.GetAwaiter().GetResult()

        # Transform the result (parse json, make compatible with soap)
        for transform in transformations:
            resultAfterWaiting = transform(resultAfterWaiting)

        return resultAfterWaiting

    @staticmethod
    def CreateAsyncResult(task, propertyName, transformations=[]):
        class Temp():
            pass

        result = Temp()

        def handler(self):
            parsedResponse = Odata.Await(task, transformations)
            return getattr(parsedResponse, propertyName)

        setattr(Temp, propertyName, property(lambda self: handler(self)))
        return result
