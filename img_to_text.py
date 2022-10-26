from PIL import Image
from PIL import ImageEnhance
from pytesseract import pytesseract
from os import listdir
from os.path import isfile, join
import cv2

path_to_tesseract = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
pytesseract.tesseract_cmd = path_to_tesseract

img_folder = 'images/'

imgs_path = [f for f in listdir(img_folder) if isfile(join(img_folder, f))]
imgs_path = [f for f in imgs_path if f.endswith(('.png', '.jpg', '.jpeg'))]

img = cv2.imread(img_folder + imgs_path[2])
img[img < 230] = 0
img = Image.fromarray(img, 'RGB')
img.show()

for img_path in imgs_path:
    img = cv2.imread(img_folder + img_path)
    img[img < 200] = 0
    img = Image.fromarray(img, 'RGB')
    img = img.convert('L')
    img = ImageEnhance.Contrast(img)
    img = img.enhance(2)
    img.show()
    try:
        text = pytesseract.image_to_string(img, lang='spa+eng', timeout=10)
        print(text)
    except RuntimeError as timeout_error:
        print('Timeout error')