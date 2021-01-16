import paddlehub as hub
from pyltp import SentenceSplitter
from tqdm import tqdm
class Senta_Model():
    def __init__(self):
        #self.model_erine = hub.Module(name = "ernie_skep_sentiment_analysis")
        self.model_bilstm = hub.Module(name = "senta_bilstm")
    #def infer_erine(self, text):
    #    results = self.model_erine.predict_sentiment(texts=[text], use_gpu=False)
    #    print(results[0]['positive_probs'])
    #    return results[0]['positive_probs']

    def infer_bilstm(self,texts):
        results = self.model_bilstm.sentiment_classify(data = {"text":[texts]})
        print(results[0]['positive_probs'])
        return results[0]['positive_probs']
if __name__ == "__main__":
    Model = Senta_Model()
    #Model.infer_erine('我一路向北，离开有你的季节，你说你好累，已无法再爱上谁')
    Model.infer_bilstm('我一路向北，离开有你的季节，你说你好累，已无法再爱上谁')

    #Model.infer_erine('开心')
    Model.infer_bilstm('开心')