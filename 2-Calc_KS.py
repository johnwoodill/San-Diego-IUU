import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import kurtosis
import multiprocessing
import glob

def process_ks(idat):
    i = pd.to_datetime(idat['timestamp'].iat[0])
    hdat = dat[(dat['timestamp'] < i ) & (dat['timestamp'] >= i - pd.Timedelta(hours=25*10) )]
    rvs1 = idat['distance'].values
    date_range = pd.to_datetime(hdat.timestamp.unique()).sort_values(ascending=False)
    retdat = pd.DataFrame()
    for j in date_range:
        # Filter last hour
        hdat2 = hdat[hdat.timestamp == j]
        rvs2 = hdat2['distance'].values
        # Calculate KS statistic
        ks_res = stats.ks_2samp(rvs1, rvs2)
        ks_stat = ks_res[0]
        pvalue = ks_res[1]

        # Time delta
        td = (i - j)/np.timedelta64(1,'h')
        indat = pd.DataFrame({'t': [i], 'lag': td, 'ks': [ks_stat], 'pvalue': [pvalue]})
        retdat = pd.concat([retdat, indat])
    retdat = retdat.reset_index(drop=True)
    retdat.to_feather(f"data/daily_ks/{i}.feather")
    print(f"Saving: data/daily_ks/{i}.feather")
    return 0




dat = pd.read_feather('data/SanDiego_Reduced_2017-08-20_2017-10-01.feather')
dat.loc[:, 'timestamp'] = pd.to_datetime(dat.timestamp)

# Remove Longbeach
# dat = dat[dat['vessel_A_lat'] <= 33.5]
# dat = dat[dat['vessel_A_lat'] >= 32]

# Group by
gb = dat.groupby('timestamp')
timest = [gb.get_group(x) for x in gb.groups]

# Parallel Process
pool = multiprocessing.Pool(50)
pool.map(process_ks, timest)
pool.close()


# Combine processed files
files = glob.glob('data/daily_ks/*.feather')


list_ = []
for file in files:
    df = pd.read_feather(file)
    list_.append(df)
    mdat = pd.concat(list_, sort=False)



mdat = mdat.reset_index(drop=True)
mdat.to_feather('data/SanDiego_KSDaily_2017-08-20_2017-10-01.feather')
