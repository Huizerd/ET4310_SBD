import boto3
import json
import matplotlib.pyplot as plt
import re

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

# Client setup --> pricing only for us-east-1
client = boto3.client('pricing', region_name='us-east-1')

# What instances to check, and which instances to put on the x and y-axis
instance_types = ['c4.large', 'c4.xlarge', 'c4.2xlarge', 'c4.4xlarge', 'c4.8xlarge',
                  'c5.large', 'c5.xlarge', 'c5.2xlarge', 'c5.4xlarge', 'c5.9xlarge', 'c5.18xlarge']
attributes = ['memory', 'ecu']

# Product filters
service_code = 'AmazonEC2'
filters = [[{'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': it}] for it in instance_types]

# Build product dict
products = {it: {att: json.loads(client.get_products(ServiceCode=service_code, Filters=filt, MaxResults=1)['PriceList'][0])['product']['attributes'][att] for att in attributes} for it, filt in zip(instance_types, filters)}

# Convert to integers
for it, att in products.items():
	for a, v in att.items():
		products[it][a] = float(re.search(r'\d*\.*\d*', v).group())

# Create figure and axis
fig, ax = plt.subplots(figsize=(8, 4))

# Plot by looping through dict
for it, att in products.items():
	ax.scatter(att[attributes[0]], att[attributes[1]], label=it)

ax.set_title(f'Characteristics of AWS EC2 instances', fontstyle='italic')
ax.set_xlabel('memory [GB]')
ax.set_ylabel('ECU [-]')
ax.legend()

fig.tight_layout()
plt.show()
