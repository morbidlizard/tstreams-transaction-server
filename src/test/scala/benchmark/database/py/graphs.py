import itertools
import os
from os import listdir
from os.path import isfile
from sys import argv

import numpy as np
import pylab as pl


def main():
    path = argv[1] + "/output"

    colors = [
        '#FF0000',
        '#00FF00',
        '#0000FF'
    ]

    filepaths = [
        path + "/" + filename
        for filename in listdir(path)
        if filename.endswith('.csv')
    ]

    for filepath, index in zip(filepaths, itertools.count()):
        data = np.genfromtxt(
            filepath, 
            delimiter="\t",
            dtype=str
        )

        filename = os.path.basename(filepath)

        without_extension = os.path.splitext(filename)[0]

        # first column (0) every second row (1::2)
        db_names = data[1::2,0]

        # get rid of first two columns (db name and metric)
        data = data[:, 2:].astype(float)
        
        # first (0) row (:)
        sizes = data[0, :]
        # 
        values = [
            # rows (:) split in pairs
            data[i * 2 - 1:i * 2 + 1,:] 
            for i in range(
                1, 
                int(data.shape[0] / 2) + 1
            )]

        dbs = values

        pl.figure(index, figsize=(6, 6))
        pl.grid(True)

        for db_name, value, color in zip(db_names, values, colors):
            db_name = db_name.split('_')[0]
            y = value[0,:]
            err = value[1,:]
            pl.plot(
                sizes,
                y,
                color,
                label=db_name,
                marker='.', ms = 10
            )
            pl.fill_between(
                sizes,
                y + err,
                np.maximum(y - err, 0),
                facecolor=color,
                alpha=0.3
            )   

        pl.legend(loc=2)
        # title from format {op}_{thread_number}_threads{shift}_shift
        op, threads, _, shift, _ = without_extension.split('_')
        op, threads, shift = op, int(threads), float(shift)
        pl.title(
            '%s%s%s' % (
                op, 
                " on %s thread(s)" % threads if threads > 0 else "", 
                " with %s% shift" % (shift * 100) if shift > 0 else ""
            )
        )

        pl.savefig(path + '/' + without_extension + ".png")


if __name__ == "__main__":
    main()
