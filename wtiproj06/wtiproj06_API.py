from flask import Flask, jsonify, request, make_response
import wtiproj06_api_logic

api=wtiproj06_api_logic.api_logic()
app = Flask(__name__)
pm,g=api.load(1)
g1=['userID','movieID','rating']

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)
@app.route('/ratings', methods=['DELETE'])
def delete():
    api.clear_table(api.cass.ratings)
    return jsonify({}), 204, {"Content-Type": "application/json"}
@app.route('/ratings', methods=["GET"])
def get():
    result=api.get_data(api.cass.ratings)
    result=result.current_rows
    zwrot=[]
    if (len(result) != 0):
        for w in result:
            zwrot.append(w)
    return jsonify(zwrot), 200, {"Content-Type": "application/json"}
@app.route('/rating', methods=['POST'])
def give():
    result = request.get_json()
    ID=result["userID"]
    api.push_data(api.cass.ratings,result)
    result2 = api.get_data(api.cass.ratings)
    zwrot = []
    result2 = result2.current_rows
    if (len(result2) != 0):
        for w in result2:
            zwrot.append(w)
        df=api.ToDf(zwrot)
        g2=g1+g
        df.columns=g2
        profile=api.user_profile(df, g, int(ID))
        profile = profile.tolist()
        api.push_data(api.cass.profiles,profile,ID)
    return jsonify(result), 201, {"Content-Type": "application/json"}
@app.route('/avg-genre-ratings/all-users', methods=["GET"])
def all_users():
    result = api.get_data(api.cass.ratings)
    zwrot = []
    result = result.current_rows
    if (len(result) != 0):
        for w in result:
            zwrot.append(w)
        df=api.ToDf(zwrot)
        g2=g1+g
        df.columns=g2
        df2,zwrot=api.avg(df,g)
        zwrot=zwrot.tolist()
    return jsonify(zwrot), 200, {"Content-Type": "application/json"}
@app.route('/avg-genre-ratings/<int:userID>', methods=["GET"])
def avg_user(userID):
    profile=api.get_data(api.cass.profiles,str(userID))
    profile=profile.current_rows
    profile=profile[0]
    profile=list(profile.values())
    del profile[0]
    return jsonify(profile), 200, {"Content-Type": "application/json"}

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=9875)
