
# please install driver first
# e.g: pip install cassandra-driver

from cassandra.cluster import Cluster

if __name__ == "__main__":
    # change hadoop  to your cassandra hosts
    cluster = Cluster(['hadoop'],port=9042)
    session = cluster.connect('store',wait_for_all_pools=True)
    session.execute('USE store')
    rows = session.execute('SELECT * FROM shopping_cart')
    for row in rows:
        print(row.userid,row.item_count,row.last_update_timestamp)
