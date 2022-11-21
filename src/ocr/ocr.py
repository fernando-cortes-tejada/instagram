import pandas as pd
from glob import glob

import easyocr


class ocrProcess:
    def __init__(self):
        self.model = None
        self.bestK = None
        pass

    def defineModel(self, list_languages, gpu=False, method=None):
        if method == "easyocr":
            self.model = easyocr.Reader(list_languages, gpu=gpu)

        return self.model

    def getText(self, img):
        result = self.model.readtext(img)
        img_id = img.split("\\")[-1].split(".")[0]
        img_df = pd.DataFrame(result, columns=["bbox", "text", "conf"])
        img_df["img_id"] = img_id
        return img_df

    @staticmethod
    def getHashtagsFromText(text_result: str):
        # initializing hashtag_list variable
        hashtag_list = []

        # splitting the text into words
        for word in text_result.split():

            # checking the first character of every word
            if word[0] == "#":

                # adding the word to the hashtag_list
                hashtag_list.append(word[1:])

        return hashtag_list

    @staticmethod
    def getTagsFromText(text: str):
        # initializing hashtag_list variable
        tag_list = []

        # splitting the text into words
        for word in text.split():

            # checking the first character of every word
            if word[0] == "@":

                # adding the word to the hashtag_list
                tag_list.append(word[1:])

        return tag_list
