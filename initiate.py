from Driver.driver import Driver
import flask
from flask import request, jsonify

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/get.hiraishin.com/result', methods=['GET'])
def api_all():
    query_parameters = request.args
    query = query_parameters.get('query')
    driver = Driver(query)
    result = driver.run()
    return jsonify(result)


if __name__ == '__main__':
    app.run()

#
# if __name__ == '__main__':
#     # query = 'Select Column1, sum(Column5) from SampleTable where Column2 =
#     "a" group by Column1 having sum(Column5) >= 12000'
#     # driver = Driver(query)
#     # driver.run()
