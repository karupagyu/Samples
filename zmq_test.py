import sys
import logging

import cv2
import time
import random

# import concurrent.futures
import threading

import zmq
import msgpack
import msgpack_numpy

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

logger = logging.getLogger(__name__)


def serialize_msgpack_numpy(obj):
    msg = msgpack.packb(obj, default=msgpack_numpy.encode)
    return msg


def deserialize_msgpack_numpy(msg):
    obj = msgpack.unpackb(msg, object_hook=msgpack_numpy.decode)
    return obj


def pub():
    camera = cv2.VideoCapture(0)
    isOpened = camera.isOpened()
    logger.info({'action': 'Open Camera',
                 'isOpened': isOpened})

    ret, frame = camera.read()

    host = '127.0.0.1'
    port = 5990
    hwm = 10

    context = zmq.Context()
    # PUBに指定します。
    sock = context.socket(zmq.PUB)
    sock.set_hwm(hwm)
    sock.bind("tcp://{}:{}".format(host, port))

    # Sub側の受信準備を待つため
    time.sleep(1)

    id = 0
    while True:
        random_topic = str(random.randint(1, 1))
        b_topic = random_topic.encode('utf-8')

        id += 1
        b_id = str(id).encode('utf-8')

        ret, frame = camera.read()
        b_frame = serialize_msgpack_numpy(frame)
        sock.send_multipart([b_topic, b_id, b_frame], flags=zmq.NOBLOCK)
        logger.info({'action': 'send', "id": id, "byte-size": len(b_frame)})

        time.sleep(1)


def sub_1():
    """
    Topicが’1’の時だけ受信します。
    """

    host = '127.0.0.1'
    port = 5990
    hwm = 10
    context = zmq.Context()
    # SUBに指定します。
    sock = context.socket(zmq.SUB)
    sock.set_hwm(hwm)

    channel = '1'.encode('utf-8')
    # 必ずSUBSCRIBERを指定します。指定しないと受け取れません。
    sock.setsockopt(zmq.SUBSCRIBE, channel)
    sock.connect("tcp://{}:{}".format(host, port))

    while True:
        [b_topic, b_id, b_frame] = sock.recv_multipart()

        id = b_id.decode('utf-8')
        frame = deserialize_msgpack_numpy(b_frame)
        logger.info({"action": "recv", "id": id})

        cv2.imshow('camera', frame)

        # キー操作があればwhileループを抜ける
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break


if __name__ == "__main__":

    # executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    # executor.submit(sub_1)
    # executor.submit(pub)

    pt = threading.Thread(target=pub)
    pt.setDaemon(True)
    pt.start()

    time.sleep(15)
    st = threading.Thread(target=sub_1)
    st.setDaemon(True)
    st.start()

    pt.join()
    st.join()
