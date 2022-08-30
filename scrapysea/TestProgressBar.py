from tqdm import tqdm, trange, tqdm_notebook
from time import sleep



def ProgressBar(limit):
    pbar = tqdm(total=100)
    for i in range(5):
        sleep(0.1)
        pbar.update(10)
    pbar.close()
        
if __name__ == "__main__":
    ProgressBar()