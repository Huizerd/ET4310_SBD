import matplotlib.pyplot as plt

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
facecolor = plt.rcParams['axes.facecolor']

# Data
partitions = [30114, 12800, 7680, 2560, 1280, 256]
runtime = [12.53, 11.53, 11.55, 11.53, 12.58, 14.53]
annotations = ['# input files', '50*dP', '30*dP',
               '10*dP', '5*dP', 'defaultParallelism']

# Create figure and axis
fig, ax = plt.subplots(figsize=(8, 4))

ax.plot(partitions, runtime, '-o')

for i in range(len(partitions)):
	ax.annotate(annotations[i], (partitions[i], runtime[i]))

ax.set_title(f'Effect of coalesce on runtime', fontstyle='italic')
ax.set_xlabel('partitions [-]')
ax.set_ylabel('runtime [min]')

fig.tight_layout()
fig.savefig('../figures/coalesce.pdf')
plt.show()
