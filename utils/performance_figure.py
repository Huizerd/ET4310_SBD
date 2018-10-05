import json

import matplotlib.pyplot as plt
import numpy as np

# Matplotlib config
plt.style.use('ggplot')
plt.rcParams['font.family'] = 'serif'
plt.rcParams['font.serif'] = 'Ubuntu'
plt.rcParams['font.monospace'] = 'Ubuntu Mono'
plt.rcParams['font.size'] = 12
plt.rcParams['axes.labelsize'] = 12
plt.rcParams['axes.labelweight'] = 'bold'
plt.rcParams['xtick.labelsize'] = 12
plt.rcParams['ytick.labelsize'] = 12
plt.rcParams['legend.fontsize'] = 12
plt.rcParams['figure.titlesize'] = 14

# Data
# runtime = [1.2, 2.5, 4.7, 19]
# files = [10, 50, 100, 500]

runtime = [1.2, 1.3, 1.3, 2.7, 2.7, 4, 5, 5, 5, 16, 16, 16, 35, 35, 35]
files = [10, 10, 10, 50, 50, 50, 100, 100, 100, 500, 500, 500, 1000, 1000, 1000]

# Extrapolate
ex = np.polyfit(files, runtime, 1)
f_ex = np.poly1d(ex)
ex2 = np.polyfit(files, runtime, 2)
f_ex2 = np.poly1d(ex2)

x_ex = [1, 10000]

# Create figure and axis
fig, ax = plt.subplots(figsize=(8, 4))

ax.plot(x_ex, f_ex(x_ex), label='linear extrapolation')
# ax.plot(x_ex, f_ex2(x_ex), label='quadratic extrapolation')
ax.plot(files, runtime, marker='o', linestyle='None', label='actual')

ax.set_title('RDD performance', fontstyle='italic')
ax.set_xlabel('number of files [-]')
ax.set_ylabel('runtime [s]')
ax.legend()

fig.tight_layout()
fig.savefig('figures/performance2.pdf')
plt.show()
