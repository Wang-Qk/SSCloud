from flask import request, Flask,render_template
from flask_restful import Api, Resource
from sentiment import Senta_Model
from key_word import TF_IDF_MODEL
import json
import torch
from types import SimpleNamespace

#encode
app = Flask(__name__)
api = Api(app)
senta_M = Senta_Model()
key_M = TF_IDF_MODEL()
with open('data/news_01-08.json','r',encoding='utf-8') as f:
    train_f = json.load(f)
    train_data = []
    for news in train_f:
        train_data.append(news['title'])
        train_data.append(news['abstract'])
    key_M.train(train_data)
    print(11111111)

class Classifier(Resource):
    def put(self):
        texts = request.form.get("text")
        posi = senta_M.infer_bilstm(texts)
        keywords = key_M.infer(texts,4)
        res = json.dumps({"sentiment":posi,"keywords":keywords},ensure_ascii=False)
        return res

api.add_resource(Classifier, "/senta")
if __name__ == '__main__':
    app.run(host='0.0.0.0',port=8087, debug=True)
