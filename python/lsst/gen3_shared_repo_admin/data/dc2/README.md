# DC2 visit lists for DP0

These visit lists were created by Jim Chiang with the following Python script (see PREOPS-581 for more information):

    import os
    import glob
    import sqlite3
    import numpy as np
    import pandas as pd

    opsim_db_file = '/global/cfs/cdirs/descssim/DC2/minion_1016_desc_dithered_v4_trimmed.db'

    query = 'select obsHistID from summary where propID={} '

    # Get 10 years of WFD and DDF visits.
    with sqlite3.connect(opsim_db_file) as con:
        wfd_set = set(pd.read_sql(query.format(54), con)['obsHistID'])
        ddf_set = set(pd.read_sql(query.format(56), con)['obsHistID'])

    # Limit to visits in raw data directory.
    raw_data_dir = '/global/cfs/cdirs/lsst/shared/DC2-prod/Run2.2i/sim'
    raw_visits = set()
    for year in range(1, 6):
        raw_visits.update([int(os.path.basename(_)) for _ in
                        glob.glob(os.path.join(raw_data_dir,
                                                f'y{year}-wfd', '*'))])

    ddf = sorted(list(ddf_set.intersection(raw_visits)))
    wfd = sorted(list(wfd_set.intersection(raw_visits)))

    print(len(raw_visits), len(ddf) + len(wfd))

    np.savetxt('DR6_Run2.2i_WFD_visits.txt', wfd, fmt='%d')
