import protobuf.data_file_pb2 as dfpb
import multiprocessing as mp
import pandas as pd
import math
import time


def gcj_to_wgs(lon, lat):
    a = 6378245.0  # 克拉索夫斯基椭球参数长半轴a
    ee = 0.00669342162296594323  # 克拉索夫斯基椭球参数第一偏心率平方
    PI = 3.14159265358979324  # 圆周率
    # 以下为转换公式
    x = lon - 105.0
    y = lat - 35.0
    # 经度
    dLon = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * math.sqrt(abs(x))
    dLon += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
    dLon += (20.0 * math.sin(x * PI) + 40.0 * math.sin(x / 3.0 * PI)) * 2.0 / 3.0
    dLon += (150.0 * math.sin(x / 12.0 * PI) + 300.0 * math.sin(x / 30.0 * PI)) * 2.0 / 3.0
    # 纬度
    dLat = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * math.sqrt(abs(x))
    dLat += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
    dLat += (20.0 * math.sin(y * PI) + 40.0 * math.sin(y / 3.0 * PI)) * 2.0 / 3.0
    dLat += (160.0 * math.sin(y / 12.0 * PI) + 320 * math.sin(y * PI / 30.0)) * 2.0 / 3.0
    radLat = lat / 180.0 * PI
    magic = math.sin(radLat)
    magic = 1 - ee * magic * magic
    sqrtMagic = math.sqrt(magic)
    dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * PI)
    dLon = (dLon * 180.0) / (a / sqrtMagic * math.cos(radLat) * PI)
    wgsLon = lon - dLon
    wgsLat = lat - dLat
    return wgsLon, wgsLat


def operate_data(gid, uid, data):
    print("processing gid {:6d} uid {}".format(gid, uid))
    data = data.loc[(data["lat"] != data["lat"].shift()) |
                    (data["lng"] != data["lng"].shift())] \
        .reset_index(drop=True)
    return uid, data


class DataFile:
    def __init__(self, message):
        self.proto = dfpb.DataFile()
        self.proto.ParseFromString(message)
        self.data = None
        self.grouped_data = None
        self.groups = None
        self.read_file()

    def update(self, message):
        new_proto = dfpb.DataFile()
        new_proto.ParseFromString(message)
        self.proto = new_proto
        if not new_proto.file == self.proto.file:
            self.read_file()

    def read_file(self):
        print("read file {}".format(self.proto.file))
        ts = time.time()
        header = None if self.proto.has_header else 0
        self.data = pd.read_csv(
            self.proto.file,
            sep=self.proto.sep,
            skiprows=header,
            error_bad_lines=False,
            header=None
        )
        print("[{:.2f}] rename columns".format(time.time()-ts))
        self.data = self.data.rename(columns={
            self.proto.row_uid: "uid",
            self.proto.row_time: "time",
            self.proto.row_lat: "lat",
            self.proto.row_lng: "lng"
        })[["uid", "time", "lat", "lng"]]
        print("[{:.2f}] drop na".format(time.time()-ts))
        self.data = self.data.dropna().reset_index(drop=True)
        print("[{:.2f}] read file complete".format(time.time()-ts))
        # self.data = self.data \
        #     .sort_values(by=["uid"], axis=0) \
        #     .loc[(self.data["uid"] == self.data["uid"].shift()) & (
        #         (self.data["lat"] != self.data["lat"].shift()) |
        #         (self.data["lng"] != self.data["lng"].shift()))] \
        #     .reset_index(drop=True)

    def group_data(self):
        print("group data")
        ts = time.time()
        self.grouped_data = []
        self.groups = []
        processes = []
        Q = mp.Queue()
        gid = 0
        tmp = self.data.groupby("uid")
        print(len(tmp))
        for uid, data in tmp:
            u, d = operate_data(gid, uid, data)
            gid+=1
            self.groups.append(u)
            self.grouped_data.append(d)
        # for uid, data in tmp:
        #     x = mp.Process(target=operate_data, args=(gid, uid, data,))
        #     gid += 1
        #     processes.append(x)
        #     if gid % 100 == 0:
        #         [x.start() for x in processes]
        #         [x.join() for x in processes]
        #         for x in processes:
        #             uid, data = x
        #             self.groups.append(uid)
        #             self.grouped_data.append(data)
        #         processes = []
        # [x.start() for x in processes]
        # [x.join() for x in processes]
        # for x in processes:
        #     uid, data = x
        #     self.groups.append(uid)
        #     self.grouped_data.append(data)
        print("group data end. time spent: {:.2f}".format(time.time() - ts))

    def gcj_to_wgs(self):
        print("converting coordinate")
        ts=time.time()
        self.data["lng"], self.data["lat"] = zip(*self.data.apply(lambda x: gcj_to_wgs(x["lng"], x["lat"]), axis=1))
        print("[{:.2f}] done".format(time.time()-ts))

    def store_data_as_file(self):
        print("saving file to {}".format(self.proto.file+"_converted"))
        self.data.to_csv(self.proto.file+"_converted")