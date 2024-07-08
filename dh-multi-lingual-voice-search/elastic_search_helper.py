from gevent import lock
from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch
import pandas as pd


class ESmanager:
    _instance = None
    _lock = lock.RLock()

    def __init__(self):
        self._instance = None
        self.nmatchingarticles = 5
        self.connection = self.__make_connection()

    @classmethod
    def get_instance(cls):
        if not ESmanager._instance:
            with cls._lock:
                if not ESmanager._instance:
                    ESmanager._instance = ESmanager()
        return ESmanager._instance

    def __make_connection(self):

        host = "10.12.4.176"
        port = "9200"
        timeout = 20

        connection = Elasticsearch(host=host, port=int(port), timeout=timeout)
        if not connection.ping():
            raise ConnectionError("Connection with elastic search not made")
        return connection

    def make_dataframe(self, artiand, artior, arti2and, arti2or):
        id_articles = []
        title_articles = []
        text_articles_1 = []
        text_lang = []
        full_text = []
        score = []
        for i in range(len(artiand["hits"])):
            id_articles.append(artiand["hits"][i]["_source"]["item_id"])
            title_articles.append(artiand["hits"][i]["_source"]["title_original"])
            text_articles_1.append(
                artiand["hits"][i]["_source"]["text_snippet_original"]
            )
            text_lang.append(artiand["hits"][i]["_source"]["lang"])
            full_text.append(artiand["hits"][i]["_source"]["text_snippet"])
            score.append(artiand["hits"][i]["_score"])
        for i in range(len(arti2and["hits"])):
            id_articles.append(arti2and["hits"][i]["_source"]["item_id"])
            title_articles.append(arti2and["hits"][i]["_source"]["title_original"])
            text_articles_1.append(
                arti2and["hits"][i]["_source"]["text_snippet_original"]
            )
            text_lang.append(arti2and["hits"][i]["_source"]["lang"])
            full_text.append(arti2and["hits"][i]["_source"]["text_snippet"])
            score.append(arti2and["hits"][i]["_score"])
        for i in range(len(artior["hits"])):
            id_articles.append(artior["hits"][i]["_source"]["item_id"])
            title_articles.append(artior["hits"][i]["_source"]["title_original"])
            text_articles_1.append(
                artior["hits"][i]["_source"]["text_snippet_original"]
            )
            text_lang.append(artior["hits"][i]["_source"]["lang"])
            full_text.append(artior["hits"][i]["_source"]["text_snippet"])
            score.append(artior["hits"][i]["_score"])
        for i in range(len(arti2or["hits"])):
            id_articles.append(arti2or["hits"][i]["_source"]["item_id"])
            title_articles.append(arti2or["hits"][i]["_source"]["title_original"])
            text_articles_1.append(
                arti2or["hits"][i]["_source"]["text_snippet_original"]
            )
            text_lang.append(arti2or["hits"][i]["_source"]["lang"])
            full_text.append(arti2or["hits"][i]["_source"]["text_snippet"])
            score.append(arti2or["hits"][i]["_score"])
        df = pd.DataFrame(
            {
                "id_articles": id_articles,
                "title_articles": title_articles,
                "text_articles_1": text_articles_1,
                "text_lang": text_lang,
                "full_text": full_text,
                "score": score,
            }
        )
        return df

    def get_matching_articles(self, keywords, index, num_of_articles):
        # query = {
        #     "query": {
        #         "multi_match": {
        #             "query": keywords,
        #             "fields": ["title", "text__snippet"],
        #             #                     'analyzer':'keyword',
        #             "type": "most_fields",
        #             "operator": "and",
        #         }
        #     }
        # }

        query = {
            "query": {
                "multi_match": {
                    "fields": ["title", "text__snippet"],
                    "query": keywords,
                    "fuzziness": "AUTO",
                    "operator": "and",
                }
            }
        }

        articles1 = self.connection.search(
            index=index, body=query, size=num_of_articles
        )
        # query = {
        #     "query": {
        #         "multi_match": {
        #             "query": keywords,
        #             "fields": ["title", "text__snippet"],
        #             "type": "most_fields",
        #             "operator": "or",
        #         }
        #     }
        # }

        query = {
            "query": {
                "multi_match": {
                    "fields": ["title", "text__snippet"],
                    "query": keywords,
                    "fuzziness": "AUTO",
                    "operator": "or",
                }
            }
        }
        articles2 = self.connection.search(
            index=index, body=query, size=num_of_articles
        )

        return articles1["hits"], articles2["hits"]

