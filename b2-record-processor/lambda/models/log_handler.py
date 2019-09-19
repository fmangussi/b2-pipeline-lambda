from logging import StreamHandler


class ElasticSearchHandler(StreamHandler):

    def emit(self, record):
        # TODO: implement the elastic search code
        # msg = self.format(record)
        # print(f"Sending it to elastic search - {msg}")
        pass
