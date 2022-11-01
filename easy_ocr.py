import pandas as pd
import numpy as np
from glob import glob
import easyocr
import matplotlib.pyplot as plt
from PIL import Image

plt.style.use('ggplot')

img_fns = glob('images/*')
image_id = img_fns[0].split('\\')[-1].split('.')[0]


reader = easyocr.Reader(['en','es'], gpu = False)
results = reader.readtext(img_fns[6])
print(pd.DataFrame(results, columns=['bbox','text','conf']))

dfs = []
for img in img_fns[:10]:
    result = reader.readtext(img)
    img_id = img.split('\\')[-1].split('.')[0]
    img_df = pd.DataFrame(result, columns=['bbox','text','conf'])
    img_df['img_id'] = img_id
    dfs.append(img_df)
easyocr_df = pd.concat(dfs)