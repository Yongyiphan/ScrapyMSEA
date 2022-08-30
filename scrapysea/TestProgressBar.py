from tqdm import tnrange, tqdm_notebook
from time import sleep



def ProgressBar(limit):
    for i in tnrange(limit):
        sleep(0.1)
        
        
if __name__ == "__main__":
    ProgressBar()