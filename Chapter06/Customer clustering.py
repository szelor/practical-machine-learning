# Load packages.
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import revoscalepy
from scipy.spatial import distance as sci_distance
from sklearn import cluster as sk_cluster

# Connection string to connect to SQL Server named instance.
conn_str = 'Driver=SQL Server;Server=MS;Database=ML;Trusted_Connection=True;'
input_query = '''SELECT * FROM [OnlineSales].[RFM]'''

# Define the columns we wish to import.
column_info = {
    "customer": {"type": "integer"},
    "orderRatio": {"type": "integer"},
    "itemsRatio": {"type": "integer"},
    "frequency": {"type": "integer"}
    }

data_source = revoscalepy.RxSqlServerData(sql_query=input_query, column_Info=column_info, connection_string=conn_str)
revoscalepy.RxInSqlServer(connection_string=conn_str, num_tasks=1, auto_cleanup=False)
# import data source and convert to pandas dataframe.
customer_data = pd.DataFrame(revoscalepy.rx_import(data_source))
print("Data frame:", customer_data.head(n=5))
print(customer_data.describe())

cdata = customer_data
K = range(1, 20)
KM = (sk_cluster.KMeans(n_clusters=k).fit(cdata) for k in K)
centroids = (k.cluster_centers_ for k in KM)

D_k = (sci_distance.cdist(cdata, cent, 'euclidean') for cent in centroids)
dist = (np.min(D, axis=1) for D in D_k)
avgWithinSS = [sum(d) / cdata.shape[0] for d in dist]
plt.plot(K, avgWithinSS, 'b*-')
plt.grid(True)
plt.xlabel('Number of clusters')
plt.ylabel('Average within-cluster sum of squares')
plt.title('Elbow for KMeans clustering')
plt.show()

# It looks like k=4 is a good number to use based on the elbow graph.
n_clusters = 4
means_cluster = sk_cluster.KMeans(n_clusters=n_clusters, random_state=111)
columns = ["orderRatio", "itemsRatio", "monetaryRatio", "frequency"]
est = means_cluster.fit(customer_data[columns])
clusters = est.labels_
customer_data['cluster'] = clusters

# Print some data about the clusters:

# For each cluster, count the members.
for c in range(n_clusters):
    cluster_members=customer_data[customer_data['cluster'] == c][:]
    print('Cluster {}(n={})'.format(c, len(cluster_members)))

# Print mean values per cluster.
print(customer_data.groupby(['cluster']).mean())