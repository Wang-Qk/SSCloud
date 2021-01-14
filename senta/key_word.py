
import pickle as pkl
import json
import sys
import os
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from pyltp import Segmentor
from pyltp import SentenceSplitter
from collections import defaultdict
import torch
sys.path.append('../')
LTP_DATA_DIR = '../RESOURCE/ltp_data_v3.4.0'  # ltp模型目录的路径
cws_model_path = os.path.join(LTP_DATA_DIR, 'cws.model')  # 分词模型路径，模型名称为`cws.model`


class TF_IDF_MODEL():
    def __init__(self):
        self.vectorizer = CountVectorizer(decode_error="replace")
        self.tfidftransformer = TfidfTransformer()
        self.segmentor = Segmentor()  # 初始化实例
        self.segmentor.load(cws_model_path)  # 加载模型
        self.word_dict = defaultdict()
    def train(self,train_data):
        train_word_list = []
        for doc in train_data:
            tokens = list(self.segmentor.segment(doc.strip()))
            train_word_list.append(' '.join(tokens))
        
        # 注意在训练的时候必须用vectorizer.fit_transform、tfidftransformer.fit_transform
        # 在预测的时候必须用vectorizer.transform、tfidftransformer.transform
        vec_train = self.vectorizer.fit_transform(train_word_list)
        tfidf = self.tfidftransformer.fit_transform(vec_train)
        for feature in self.vectorizer.get_feature_names():
            self.word_dict[len(self.word_dict)] = feature

        feature_path = 'models/feature.pkl'
        with open(feature_path, 'wb') as fw:
            pkl.dump(self.vectorizer.vocabulary_, fw)

        tfidftransformer_path = 'models/tfidftransformer.pkl'
        with open(tfidftransformer_path, 'wb') as fw:
            pkl.dump(self.tfidftransformer, fw)

    def infer(self,doc,k):
        tokens = list(self.segmentor.segment(doc.strip()))
        word_list = ' '.join(tokens)
        test_tfidf = self.tfidftransformer.transform(self.vectorizer.transform([word_list])).toarray()[0]
        _, idx = torch.FloatTensor(test_tfidf).topk(k)

        key_words = []
        for i in idx:
            print(self.word_dict[i.item()] + ": " + str(test_tfidf[i]))
            key_words.append(self.word_dict[i.item()])
        return key_words
    
if __name__ == '__main__':
    model = TF_IDF_MODEL()
    with open('data/news_01-08.json','r',encoding='utf-8') as f:
        train_f = json.load(f)
        train_data = []
        for news in train_f:
            train_data.append(news['title'])
            train_data.append(news['abstract'])

    model.train(train_data)
    model.infer('中国恒大新能源汽车港交所公告，时守明先生已辞任其于本公司执行董事及董事会董事长的职位',4)