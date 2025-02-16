import requests


def send_http_request(query):
    # Define the target URL
    # url = "https://example.com/api"
    # url = "http://localhost:7025"
    # url = "http://127.0.0.1:7025"
    url = "http://prut.informatik.privat:7025"

    # Define the headers
    headers = {
        # "Content-Type": "application/x-www-form-urlencoded",
        # "Authorization": "Bearer YOUR_ACCESS_TOKEN"
        "Accept": "text/tab-separated-values"
    }

    # Define the URL-encoded form data
    data = {
        #"param1": "value1",
        #"param2": "value2"
        "query": query
    }

    # Send the POST request
    response = requests.post(url, headers=headers, data=data)

    # Print response
    # print("Status Code:", response.status_code)
    # print("Response Body:", response.text)


def create_query(maxDist, latitude, longitude, geotype1, geotype2, sizeChildLeft, sizeChildRight, algorithm, limit):
    def getGeoType(geotype, varName=""):
        if geotype == 'point':
            return '<true> '
        elif geotype == 'area':
            return '<false> '
        else:
            return varName + ' '
    
    def addSizeRestrictions(sizeChild, varName):
        restrictions = '{ '
        if sizeChild == 0:
            restrictions += '' # no constraints
        if sizeChild >= 1:
            restrictions += varName + ' <lon-is-div-by> <two> . '
        if sizeChild >= 2:
            restrictions += varName + ' <lat-is-div-by> <two> . '
        if sizeChild >= 3:
            restrictions += varName + ' <lon-is-div-by> <three> . '
        if sizeChild >= 4:
            restrictions += varName + ' <lat-is-div-by> <three> . '
        if sizeChild >= 5:
            restrictions += varName + ' <lon-is-div-by> <four> . '
        if sizeChild >= 6:
            restrictions += varName + ' <lat-is-div-by> <four> . '
        if sizeChild >= 7:
            restrictions += varName + ' <lon-is-div-by> <five> . '
        if sizeChild >= 8:
            restrictions += varName + ' <lat-is-div-by> <five> . '
        restrictions += '} '
        return restrictions
            
    
    query = "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/> "
    query += "SELECT ?a ?b ?dist WHERE { "
    
    # add content to query
    # query += '?a ?b ?c .'  # for testing only
    
    query += "?a <isPoint>" + getGeoType(geotype1, "?isPointA") + ". "
    query += "?a <asWKT> ?wktA . "
    query += "?b <isPoint> " + getGeoType(geotype2, "?isPointB") + " . "
    query += "?b <asWKT> ?wktB . "

    query += addSizeRestrictions(sizeChildLeft, "?a")
    query += addSizeRestrictions(sizeChildRight, "?b")

    # add spatialJoin
    query += "SERVICE spatialSearch: { "
    query += "_:config spatialSearch:algorithm spatialSearch:" + algorithm + " ; "
    query += "spatialSearch:left ?wktA ; "
    query += "spatialSearch:right ?wktB ; "
    query += "spatialSearch:maxDistance " + str(maxDist) + " ; "
    query += "spatialSearch:bindDistance ?dist . "
    query += "} "
    
    query += "} "
    if limit:
        query += "LIMIT 20"
    return query


# parameters
maxDistances = [10**i for i in range(0, 8)]
latitudes = [i for i in range(-89, 90)]
longitudes = [i for i in range(-180, 181)]
geotype = ['point', 'area', 'both']
sizeChild = [i for i in range(0, 9)]
algorithm = ['baseline', 'boundingBox']

# just for testing:
#query = create_query(maxDistances[6], latitudes[89], longitudes[180], geotype[2], geotype[2], sizeChild[0], sizeChild[2], algorithm[1], True)
#send_http_request(query)
#print(query)

# ==================== test all combinations for building the smaller or larger rtree =============================
#test if it is more efficient to build the smaller or the bigger child
# for maxDist in maxDistances:
#     for sizeChildLeft in sizeChild:
#         for sizeChildRight in sizeChild:
#             if sizeChildRight > sizeChildLeft:
#                 continue  # only test the cases, where the left child is smaller or equal to the right child. The other cases are symmetric
#             query = create_query(maxDist, None, None, geotype[2], geotype[2], sizeChildLeft, sizeChildRight, algorithm[1], False)
#             send_http_request(query)

# add missing data, when both are equal
# for maxDist in maxDistances:
#     for size in sizeChild:
#         query = create_query(maxDist, latitudes[89], longitudes[180], geotype[2], geotype[2], size, size, algorithm[1], False)
#         send_http_request(query)
# ==================== end of test all combinations for building the smaller or larger rtree =============================

# ================== test baseline algorithm ==================================
for maxDist in maxDistances:
    for sizeChildLeft in sizeChild:
        for sizeChildRight in sizeChild:
            if sizeChildRight > sizeChildLeft:
                continue
            query = create_query(maxDist, None, None, geotype[2], geotype[2], sizeChildLeft, sizeChildRight, algorithm[0], False)
            send_http_request(query)