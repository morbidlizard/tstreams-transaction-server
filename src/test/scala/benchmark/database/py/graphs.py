from os import listdir
from os.path import isfile

import numpy as np

import pylab as pl


PATH = "/home/rakhimovvv/trans/bm/output"


def main():
    filenames = [PATH + "/" + filename for filename in listdir(PATH) if filename.endswith('.csv')]

    for filename in filenames:
        data = np.genfromtxt(filename, delimiter="\t")
        data = data[:, 2:]

        sizes = data[0, :]

        values = [data[i * 2 - 1:i * 2 + 1,:] for i in range(1, int(data.shape[0] / 2) + 1)]

        dbs = values

        pl.figure(1, figsize=(6, 6))
        for db in dbs:
            y = db[0,:]
            err = db[1,:]
            pl.plot(sizes, y)
            pl.fill_between(sizes, y + err, y - err, facecolor='#7EFF99', alpha=0.5)

        without_extension = '.'.join(filename.split('.')[:-1])
        pl.savefig(without_extension + ".png")


if __name__ == "__main__":
    main()
