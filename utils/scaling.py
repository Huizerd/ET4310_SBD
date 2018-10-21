import json

import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import curve_fit
from scipy.interpolate import interp1d

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
colors = [pc['color'] for pc in plt.rcParams['axes.prop_cycle']]

# Data
# runtime = [1.2, 2.5, 4.7, 19]
# files = [10, 50, 100, 500]

# c5.xlarge: month, year, all
runtime_month = [2.0, 1.4, 0.8, 0.8, 0.6]
runtime_year = [18.8, 9.7, 6.5, 5.0, 4.2]
runtime_all = [87.4, 43.3, 29.2, 22.7, 18.4]

instances = [9, 18, 27, 36, 45]
new = np.linspace(instances[0], instances[-1], num=1001, endpoint=True)

# Interpolate
f_month = interp1d(instances, runtime_month, kind='linear')
f_year = interp1d(instances, runtime_year, kind='linear')
f_all = interp1d(instances, runtime_all, kind='linear')

# p_month = np.polyfit(instances, runtime_month, 3)
# f_month = np.poly1d(p_month)
# p_year = np.polyfit(instances, runtime_year, 3)
# f_year = np.poly1d(p_year)
# p_all = np.polyfit(instances, np.log(runtime_all), 1, w=np.sqrt(runtime_all))
# f_all = np.poly1d(p_all)
# f_all = np.exp(p_all[1]) * np.exp(p_all[0] * new)
# p_all = curve_fit(lambda t, a, b: a * np.exp(b * t), instances, runtime_all, p0=(100, -	0.01))
# f_all = p_all[0][0] * np.exp(p_all[0][1] * new)

# Create figure and axis
fig, ax = plt.subplots(figsize=(6, 4))

ax.plot(new, f_month(new), label='1 month')
ax.plot(new, f_year(new), label='1 year')
ax.plot(new, f_all(new), label='all')

ax.set_title('Scaling: c5.xlarge', fontstyle='italic')
ax.set_xlabel('instances [-]')
ax.set_ylabel('runtime [min]')
ax.legend()

ax.scatter(instances, runtime_month, color=colors[0])
ax.scatter(instances, runtime_year, color=colors[1])
ax.scatter(instances, runtime_all, color=colors[2])

fig.tight_layout()
fig.savefig('../figures/c5_xlarge_scaling.pdf')
plt.show()

# c4.8xlarge: month, year, all
runtime_month = [2.4, 1.2, 0.9, 0.7, 0.6]
runtime_year = [22.0, 10.5, 7.3, 5.7, 4.5]
runtime_all = [62.0, 33.4, 26.1, 20.7]

instances = [1, 2, 3, 4, 5]
instances_all = [2, 3, 4, 5]
new = np.linspace(instances[0], instances[-1], num=1001, endpoint=True)
new_all = np.linspace(instances_all[0], instances_all[-1], num=1001, endpoint=True)

# Interpolate
f_month = interp1d(instances, runtime_month, kind='linear')
f_year = interp1d(instances, runtime_year, kind='linear')
f_all = interp1d(instances_all, runtime_all, kind='linear')

# def f_all(x, a, b, c, d):
#   return a + b * x + c * x ** 2 + d * x ** 3

# p_all = np.polyfit(instances_all, runtime_all, 3)
# f_all = np.poly1d(p_all)

# p_all, _ = curve_fit(f_all, instances_all, runtime_all)

# p_all = curve_fit(lambda t, a, b: a * np.exp(b * t), instances_all, runtime_all, p0=(100, -	0.01))
# f_all = p_all[0][0] * np.exp(p_all[0][1] * new)

# p_all = np.polyfit(np.log(instances_all), runtime_all, 1, w=np.sqrt(runtime_all))
# f_all = p_all[0] * np.log(new_all) + p_all[1]

# Create figure and axis
fig, ax = plt.subplots(figsize=(6, 4))

ax.plot(new, f_month(new), label='1 month')
ax.plot(new, f_year(new), label='1 year')
ax.plot(new_all, f_all(new_all), label='all')

ax.set_title('Scaling: c4.8xlarge', fontstyle='italic')
ax.set_xlabel('instances [-]')
ax.set_ylabel('runtime [min]')
ax.legend()

ax.scatter(instances, runtime_month, color=colors[0])
ax.scatter(instances, runtime_year, color=colors[1])
ax.scatter(instances_all, runtime_all, color=colors[2])

fig.tight_layout()
fig.savefig('../figures/c4_8xlarge_scaling.pdf')
plt.show()
