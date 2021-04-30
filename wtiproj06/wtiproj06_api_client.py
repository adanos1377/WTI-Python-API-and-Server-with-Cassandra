import requests
import wtiproj06_api_logic
import time
import json
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

def print_request_and_response_info_for_POST_or_PUT(request):
    print("---------------------------------------------------------------")
    print("request.url: ",request.url)
    print("request.status_code: ",request.status_code)
    print("request.headers: ",request.headers)
    print("request.text: ",request.text)
    print("request.request.body: ",request.request.body)
    print("request.request.headers: ",request.request.headers)
    print("---------------------------------------------------------------")

def print_request_and_response_info_for_GET_or_DELETE(request):
    print("---------------------------------------------------------------")
    print("request.url: ",request.url)
    print("request.status_code: ",request.status_code)
    print("request.headers: ",request.headers)
    print("request.text: ",request.text)
    print("request.request.headers: ",request.request.headers)
    print("---------------------------------------------------------------")

if __name__=="__main__":
    host_IP="127.0.0.1"
    dummy_user_ID = 75
    API_port_number = 9898

    api = wtiproj06_api_logic.api_logic()
    list_of_keys_of_non_ints = ["rating"]
    DFUserRatedMoviesWithMovieGenres,genres = api.load(100)
    row_iterator = DFUserRatedMoviesWithMovieGenres.iterrows()
    request = requests.delete("http://" + host_IP + ":" + str(API_port_number) + "/ratings")
    print_request_and_response_info_for_GET_or_DELETE(request)
    for indes, row in row_iterator:
        dict_for_dummy_JSON_with_rating = json.loads(row.to_json(orient='columns'))
        request = requests.post("http://" + host_IP + ":" + str(API_port_number) + "/rating", json=dict_for_dummy_JSON_with_rating)
        print_request_and_response_info_for_POST_or_PUT(request)
        #time.sleep(1)

    request = requests.get("http://" + host_IP + ":" + str(API_port_number) + "/avg-genre-ratings/all-users")
    print_request_and_response_info_for_GET_or_DELETE(request)
    request = requests.get("http://" + host_IP + ":" + str(API_port_number) + "/avg-genre-ratings/" + str(dummy_user_ID))
    print_request_and_response_info_for_GET_or_DELETE(request)

    request = requests.get("http://" + host_IP + ":" + str(API_port_number) + "/ratings")
    print_request_and_response_info_for_GET_or_DELETE(request)

    request = requests.delete("http://" + host_IP + ":" + str(API_port_number) + "/ratings")
    print_request_and_response_info_for_GET_or_DELETE(request)

    request = requests.get("http://" + host_IP + ":" + str(API_port_number) + "/ratings")
    print_request_and_response_info_for_GET_or_DELETE(request)
    # row_iterator = DFUserRatedMoviesWithMovieGenres.iterrows()
    # request = requests.delete("http://" + host_IP + ":" + str(API_port_number) + "/ratings")
    #
    # for indes, row in row_iterator:
    #     dict_for_dummy_JSON_with_rating = json.loads(row.to_json(orient='columns'))
    #     request = requests.post("http://" + host_IP + ":" + str(API_port_number) + "/rating", json=dict_for_dummy_JSON_with_rating)
    #     time[API_port_number].append(request.elapsed.total_seconds())

