from flask_restful import abort, Resource, request
import requestHandler as handler

from flask import Flask
from flask_restful import Api

# Flask 인스턴스 생성
app = Flask(__name__)
api = Api(app)


def Run(address):
    app.run(host=address, debug=True)


def abort_if_parameter_doesnt_exist(parameter):
    abort(404, message="Requested parameter {} doesn't exist".format(parameter))


def abort_if_category_doesnt_exist(category):
    abort(404, message="Requested category {} doesn't exist".format(category))


# 할일 리스트(Todos)
# Get, POST 정의
def get():
    return handler.ONCUE_API


class Kubernetes_RestAPI(Resource):
    def get(self, namespace, category):
        # get resource from kube API.
        return handler.load_resource(namespace, category, abort_if_category_doesnt_exist)

    def put(self, namespace, category):
        print("Kubernetes_RestAPI - PUT ", namespace, "/", category)
        return handler.apply_resource(request.json, category, namespace, abort_if_category_doesnt_exist)


## URL Router에 맵핑한다.(Rest URL정의)
api.add_resource(Kubernetes_RestAPI, '/oncue/<string:namespace>/<string:category>')
