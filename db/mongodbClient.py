from pymongo import MongoClient
from datetime import datetime


class MongoDbClient:
    def __init__(self, uri="mongodb://localhost:27017", db_name="proxyPool", collection_name="proxies"):
        """
        初始化 MongoDB 客户端
        :param uri: MongoDB 连接 URI
        :param db_name: 数据库名称
        :param collection_name: 集合名称
        """
        self.client = MongoClient(uri)
        self.db_name = db_name
        self.collection_name = collection_name
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        self._ensure_indexes()

    def _ensure_indexes(self):
        """创建索引"""
        self.collection.create_index("proxy", unique=True)  # 保证 proxy 唯一
        self.collection.create_index("last_check_time")  # 优化查询

    def get(self):
        """
        随机返回一个 proxy
        :return: 随机代理文档
        """
        proxy = self.collection.aggregate([{"$sample": {"size": 1}}])
        return next(proxy, None)

    def put(self, proxy):
        """
        存入一个 proxy
        :param proxy: 代理 IP:PORT 字符串
        """
        try:
            self.collection.insert_one({
                "proxy": proxy,
                "score": 10,  # 初始评分
                "last_check_time": datetime.utcnow()
            })
            print(f"代理 {proxy} 添加成功。")
        except Exception as e:
            print(f"代理添加失败：{e}")

    def pop(self):
        """
        顺序返回并删除一个 proxy
        :return: 删除的代理
        """
        proxy = self.collection.find_one_and_delete({}, sort=[("_id", 1)])
        return proxy

    def update(self, proxy, score=None):
        """
        更新指定 proxy 的信息
        :param proxy: 代理 IP:PORT
        :param score: 更新的评分值（可选）
        """
        update_fields = {"last_check_time": datetime.utcnow()}
        if score is not None:
            update_fields["score"] = score
        result = self.collection.update_one({"proxy": proxy}, {"$set": update_fields})
        if result.matched_count:
            print(f"代理 {proxy} 更新成功。")
        else:
            print(f"代理 {proxy} 更新失败。")

    def delete(self, proxy):
        """
        删除指定 proxy
        :param proxy: 代理 IP:PORT
        """
        result = self.collection.delete_one({"proxy": proxy})
        if result.deleted_count:
            print(f"代理 {proxy} 已删除。")
        else:
            print(f"代理 {proxy} 不存在或删除失败。")

    def exists(self, proxy):
        """
        判断指定 proxy 是否存在
        :param proxy: 代理 IP:PORT
        :return: 布尔值
        """
        return self.collection.count_documents({"proxy": proxy}) > 0

    def getAll(self):
        """
        返回所有代理
        :return: 代理列表
        """
        return list(self.collection.find({}, {"_id": 0}))

    def clean(self):
        """
        清除所有 proxy 信息
        """
        result = self.collection.delete_many({})
        print(f"已清除 {result.deleted_count} 个代理。")

    def getCount(self):
        """
        返回 proxy 统计信息
        :return: 代理数量
        """
        return self.collection.count_documents({})

    def changeTable(self, name):
        """
        切换操作对象（集合）
        :param name: 新集合名称
        """
        self.collection_name = name
        self.collection = self.db[self.collection_name]
        self._ensure_indexes()
        print(f"切换到集合: {name}")


# 示例用法
#if __name__ == "__main__":
#    db = MongoDbClient()
#
#    # 添加代理
#    db.put("192.168.1.100:8080")
#    db.put("192.168.1.101:8081")
#
#    # 随机获取代理
#    print("随机获取代理:", db.get())
#
#    # 删除并获取代理
#    print("删除并返回代理:", db.pop())
#
#    # 检查代理是否存在
#    print("代理是否存在:", db.exists("192.168.1.100:8080"))
#
#    # 更新代理评分
#    db.update("192.168.1.100:8080", score=5)
#
#    # 获取所有代理
#    print("所有代理:", db.getAll())
#
#    # 清除所有代理
#    db.clean()
#
#    # 统计代理数量
#    print("代理数量:", db.getCount())
#
#    # 切换集合
#    db.changeTable("newProxies")
