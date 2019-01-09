import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn import datasets
import numpy as np

# Data reductuction
digits = datasets.load_digits()
print(digits.data.shape)
plt.imshow(digits.images[4], cmap=plt.cm.gray_r)
plt.show()

kmeans = KMeans(n_clusters=10, random_state=0)
clusters = kmeans.fit_predict(digits.data)
print(kmeans.cluster_centers_.shape)

centers = kmeans.cluster_centers_.reshape(10, 8, 8)
plt.imshow(centers[0] ,cmap=plt.cm.gray_r)
plt.show()

print(digits.images[4])
print(digits.data[4])

# k-means visualized
iris = datasets.load_iris()
x = iris.data
x = np.delete(x, np.s_[0:2], axis=1)
kmeans = KMeans(n_clusters = 3, init = 'k-means++', max_iter = 300, n_init = 10, random_state = 0)

y = kmeans.fit_predict(x)
plt.scatter(x[y == 0, 0], x[y == 0, 1], s = 200, c = 'red', label = 'Cluster 1', marker= '*')
plt.scatter(x[y == 1, 0], x[y == 1, 1], s = 200, c = 'blue', label = 'Cluster 2', marker = '.')
plt.scatter(x[y == 2, 0], x[y == 2, 1], s = 200, c = 'green', label = 'Cluster 3', marker= '+')
centers = kmeans.cluster_centers_
plt.scatter(centers[:, 0], centers[:, 1], c='black', s=200, alpha=0.8, label = 'Cluster center', marker= 'o');
plt.legend()
plt.show()


