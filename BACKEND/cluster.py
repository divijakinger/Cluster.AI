import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import davies_bouldin_score


def kmeans(k, data):
    cluster = KMeans(n_clusters=k)
    cluster.fit_predict(data)
    return cluster.labels_, cluster.cluster_centers_


def select_columns(df1, names):
    new_dataset = pd.DataFrame(names)
    new_dataset = new_dataset.reindex(range(len(df1[names].values)))
    for name in names:
        new_dataset[name] = df1[name].values
    return new_dataset


def cluster_analyzer(dataframe, search_columns, rules):
    le_name_mapping = {}
    score_list = []
    if rules != "":
        dataframe.query(rules, inplace=True)
    columns_list = dataframe.columns
    for column in columns_list:
        if dataframe[column].dtype == object:
            le = LabelEncoder()
            dataframe[column] = le.fit_transform(dataframe[column])
            le_name_mapping[column] = dict(zip(le.classes_, le.transform(le.classes_)))
    new_dataset = select_columns(dataframe, search_columns)
    new_dataset = new_dataset.drop(0, axis=1)
    for i in range(2, 5):
        labels, dummy = kmeans(i, new_dataset)
        score_list.append(davies_bouldin_score(new_dataset, labels))
    numclusts = min(score_list)
    numclusts = score_list.index(numclusts)
    labels, centers = kmeans(numclusts + 2, new_dataset)
    centers_dict = {}
    for i in range(numclusts + 2):
        centers_dict[i] = centers[i].tolist()
    dataframe["cluster"] = labels
    labels_test = {}
    for i in range(numclusts + 2):
        labels_test[f"{i}"] = new_dataset[labels == i].values.tolist()
    pie_chart = {}
    for i in range(numclusts + 2):
        pie_chart[i] = len(labels_test[f"{i}"])
    return labels_test, dataframe, centers_dict, pie_chart
