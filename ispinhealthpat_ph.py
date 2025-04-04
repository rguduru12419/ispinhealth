# -*- coding: utf-8 -*-
"""ispinhealthpat_ph.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1Fka15X30ob0EbyTC01HXcGqSZcOEC9MX
"""

import pandas as pd

pat_phy=pd.read_csv('/content/sample_data/Phy-Pat-Combined Report.csv')

# @title Practice Name vs S.NO

from matplotlib import pyplot as plt
import seaborn as sns
figsize = (12, 1.2 * len(pat_phy['Practice Name'].unique()))
plt.figure(figsize=figsize)
sns.violinplot(pat_phy, x='S.NO', y='Practice Name', inner='stick', palette='Dark2')
sns.despine(top=True, right=True, bottom=True, left=True)

from collections import deque
ls=[1,2,3,4,5]
q=deque(ls)
#q.rotate(-2)
#q
q.rotate(-1)
q

q.rotate(-1)
q

ch=['*']
ch*2
ch*3

pat_phy.groupby('Practice Name').count()

pat_phy.groupby('Patient Name').count()

pat_phy.groupby(['Practice Name','Patient Name']).count()

pat_phy.groupby('Physician Name').count()

pat_phy.groupby(['Practice Name','Physician Name']).count()



pat_phy[pat_phy['Physician Name']=='Rehan Shah']

pat_phy[pat_phy['Physician Name']=='Shariq Saghir']