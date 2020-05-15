import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import kurtosis
from tqdm import tqdm
tqdm.pandas()

dat = pd.read_feather(f"data/v0.51/all_dist_matrix_{NAGENTS}_{ie}.feather")



def anom_det(idat):
    i = idat['t'].iat[0]
    if i == 0:
        return None
    h1 = idat
    h1 = h1.drop(columns='t')
    keep = np.triu(np.ones(h1.shape)).astype('bool').reshape(h1.size)
    rvs1 = h1.stack()[keep].values
    h2 = dat[(dat.t < i ) & (dat.t >= i - 25*8)]
    retdat = pd.DataFrame()
    for j in range(max(h2.t), min(h2.t), - 1):
        # Filter last hour
        h3 = h2[h2.t == j]
        h3 = h3.drop(columns='t')
        keep = np.triu(np.ones(h3.shape)).astype('bool').reshape(h3.size)
        rvs2 = h3.stack()[keep].values
        # Calculate KS statistic
        kss1 = stats.ks_2samp(rvs1, rvs2)
        ks_stat1 = kss1[0]
        pvalue1 = kss1[1]
        indat = pd.DataFrame({'t': [i], 'lag': i - j, 'ks': [ks_stat1], 'pvalue': [pvalue1]})
        retdat = pd.concat([retdat, indat])
    return retdat


# test = dat[dat.t >= 700]
odat = dat.groupby('t', as_index=False).progress_apply(lambda x: anom_det(x))

odat = odat.reset_index(drop=True)
odat.to_feather(f"data/v0.51/ks_data_{NAGENTS}_{ie}.feather")
