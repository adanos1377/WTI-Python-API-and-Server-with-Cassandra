import requests
import wtiproj05_api_logic
import time
import json

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
    API_port_number = 9875
    dummy_user_ID = 75

    '''1'''
    api = wtiproj05_api_logic.api_logic()
    list_of_keys_of_non_ints = ["rating"]
    DFUserRatedMoviesWithMovieGenres,genres = api.load(10)
    row_iterator = DFUserRatedMoviesWithMovieGenres.iterrows()
    for indes, row in row_iterator:
        dict_for_dummy_JSON_with_rating = json.loads(row.to_json(orient='columns'))
        request = requests.post("http://" + host_IP + ":" + str(API_port_number) + "/rating", json=dict_for_dummy_JSON_with_rating)
        print_request_and_response_info_for_POST_or_PUT(request)
        time.sleep(1)
    request = requests.get("http://" + host_IP + ":" + str(API_port_number) + "/ratings")
    print_request_and_response_info_for_GET_or_DELETE(request)


    '''2'''
    request = requests.get("http://" + host_IP + ":" + str(API_port_number) + "/avg-genre-ratings/all-users")
    print_request_and_response_info_for_GET_or_DELETE(request)
    request = requests.get("http://" + host_IP + ":" + str(API_port_number) + "/avg-genre-ratings/" + str(dummy_user_ID))
    print_request_and_response_info_for_GET_or_DELETE(request)
    request = requests.delete("http://" + host_IP + ":" + str(API_port_number) + "/ratings")
    print_request_and_response_info_for_GET_or_DELETE(request)
    request = requests.get("http://" + host_IP + ":" + str(API_port_number) + "/ratings")
    print_request_and_response_info_for_GET_or_DELETE(request)

