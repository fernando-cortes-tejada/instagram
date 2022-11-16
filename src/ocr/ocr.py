import pandas as pd
from glob import glob

import easyocr

class ocrProcess():
    def __init__(self):
        self.model = None 
        self.bestK = None
        pass

    def defineModel(self, list_languages, gpu=False, method=None):
        if method=='easyocr':
            self.model = easyocr.Reader(list_languages, gpu = gpu) 
        
        return self.model

    def getText(self, img):
        result = self.model.readtext(img)
        img_id = img.split('\\')[-1].split('.')[0]
        img_df = pd.DataFrame(result, columns=['bbox','text','conf'])
        img_df['img_id'] = img_id
        return img_df

    def getTextFromBatch(self, filepath):
        img_dict = {}
        img_fns = glob(filepath)
        for img in img_fns:
            result = self.getText(img=img)
            id = result['img_id'].iloc[0]
            text = result.text.str.cat(sep=' ')
            img_dict[id]=text
        return img_dict
