import pandas as pd

url = "https://drive.google.com/uc?export=download&id=1KMKux-KRF_1oQFWBNstVwXKxyRmXivWt"
data = pd.read_csv(url, delimiter="\t")

print(data.head(3))
