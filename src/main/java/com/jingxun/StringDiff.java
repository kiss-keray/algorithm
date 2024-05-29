package com.jingxun;

import cn.hutool.core.util.RandomUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author by keray
 * date:2021/3/20 4:14 下午
 * 参考: https://cloud.tencent.com/developer/article/2367282
 * 字符串diff算法
 */
public class StringDiff {

    // 找到变更轨迹
    public static byte[] myers(Callback callback, byte splitLen) {
        Map<Integer, long[]> kv = new LinkedHashMap<>();
        Map<Integer, long[]> kvPath = new LinkedHashMap<>();
        kv.put(0, new long[]{0L});
        List<Long> newPoint = new ArrayList<>();
        List<Long> newPointPath = new ArrayList<>();
        int d = 0;
        long now = System.currentTimeMillis();
        System.out.println("start");
        int[] range = new int[]{0, 0};
        for (; d < callback.getOldListSize() + callback.getNewListSize(); d++) {
            long[] lastPoint = kv.get(d);
            newPoint.clear();
            newPointPath.clear();
            boolean haveEnd = false;
            for (long p : lastPoint) {
                int[] point = longVl2Point(p);
                int x = point[0];
                int y = point[1];
                int mj = x * y;
                int rmj = range[0] * range[1];
                if (mj < rmj) {
                    continue;
                } else if (mj == rmj && x > range[0]) {
                    continue;
                } else {
                    range[0] = x;
                    range[1] = y;
                }
                // 当前点再走一步
                haveEnd |= pointGo(x, y, callback, newPoint, newPointPath, 0, p);
            }
            long[] result = listLong2Array(newPoint);
            long[] result1 = listLong2Array(newPointPath);
            kv.put(d + 1, result);
            kvPath.put(d + 1, result1);
            if (haveEnd) {
                break;
            }
        }
        System.out.println("计算路径耗时：" + (System.currentTimeMillis() - now));
        now = System.currentTimeMillis();
        // 最优路径回溯
        List<int[]> maxPath = new LinkedList<>();
        toFlashBack(kv, kvPath, d + 1, callback.getOldListSize(), callback.getNewListSize(), maxPath);
        System.out.println("回溯路径耗时：" + (System.currentTimeMillis() - now));
//        System.out.println(maxPath.stream().map(a -> String.format("(%s,%s)", a[0], a[1])).collect(Collectors.joining("=>")));
        List<Long> path = new ArrayList<>();
        now = System.currentTimeMillis();
        for (int i = 1; i < maxPath.size(); i++) {
            pathData2(maxPath.get(i - 1)[0], maxPath.get(i - 1)[1], maxPath.get(i)[0], maxPath.get(i)[1], callback, path, 0, 0, callback.getOldListSize(), callback.getNewListSize());
        }
        path.add(point2Long(callback.getOldListSize(), callback.getNewListSize()));
        System.out.println("生成路径耗时：" + (System.currentTimeMillis() - now));
//        System.out.println(path.stream().map(e -> {
//            int[] a = longVl2Point(e);
//            return String.format("(%s,%s)", a[0], a[1]);
//        }).collect(Collectors.joining("=>")));
        now = System.currentTimeMillis();
        byte[] r = pathData(path, callback, splitLen);
        System.out.println("生成结果耗时：" + (System.currentTimeMillis() - now));
        return r;
    }

    // type 0 右走 1下走 2斜走
    // 00 31位 31位
    private static byte[] pathData(List<Long> path, Callback callback, byte splitLen) {
        List<byte[]> result = new LinkedList<>();
        // 0 右 1 下 2 斜
        byte type = 3;
        int[] last = null;
        int count = 0;
        int bitSetLength = 0;
        for (long p : path) {
            count++;
            int[] point = longVl2Point(p);
            if (last != null) {
                if (point[1] == last[1]) {
                    byte[] addData = callback.sourceContent(point[0] - 1);
                    if (addData.length > 0x3f) {
                        throw new IllegalStateException("callback.targetContent 返回的字节数组大于0x3f");
                    }
                    bitSetLength += pathDataChange(type, count, result);
                    bitSetLength += (addData.length + 1);
                    type = 0;
                    count = 0;
                    System.out.println("删除：字符=" + new String(addData));
                    byte[] bytes = new byte[addData.length + 1];
                    bytes[0] = (byte) addData.length;
                    System.arraycopy(addData, 0, bytes, 1, addData.length);
                    result.add(bytes);
                } else if (point[0] == last[0]) {
                    if (type == 3) {
                        type = 1;
                        count = 0;
                    }
                    if (type != 1) {
                        bitSetLength += pathDataChange(type, count, result);
                        count = 0;
                        type = 1;
                    }
                } else {
                    if (type == 3) {
                        type = 2;
                        count = 0;
                    }
                    if (type != 2) {
                        bitSetLength += pathDataChange(type, count, result);
                        count = 0;
                        type = 2;
                    }
                }
            }
            last = point;
        }
        bitSetLength += pathDataChange(type, count + 2, result);
        byte[] resultBytes = new byte[bitSetLength + 2];
        System.out.println("===========:" + resultBytes.length);
        // 当前版本 0x01
        resultBytes[0] = 0x01;
        resultBytes[1] = splitLen;
        int index = 2;
        for (byte[] data : result) {
            System.arraycopy(data, 0, resultBytes, index, data.length);
            index += data.length;
        }
        return resultBytes;
    }

    private static int pathDataChange(byte type, int count, List<byte[]> result) {
        if (type == 1) {
            System.out.println("添加：长度=" + count);
            result.add(new byte[]{(byte) (0x40 | count >> 24), (byte) (count >> 16), (byte) (count >> 8), (byte) count});
            return 4;
        } else if (type == 2) {
            System.out.println("不变：长度=" + count);
            result.add(new byte[]{(byte) ((byte) 0x80 | count >> 24), (byte) (count >> 16), (byte) (count >> 8), (byte) count});
            return 4;
        }
        return 0;
    }


    private static boolean pathData2(int x, int y, int x1, int y1, Callback callback, List<Long> result, int leave, int step, int maxX, int maxY) {
        if (x == x1 && y == y1) {
            return true;
        }
        if (step > 1 || y > maxY || x > maxX) {
            return false;
        }
        List<Long> childrenResult;
        if (leave == 0) {
            childrenResult = new ArrayList<>();
        } else {
            childrenResult = result;
        }
        if (x < maxX && y < maxY && callback.areItemsTheSame(x, y)) {
            if (pathData2(x + 1, y + 1, x1, y1, callback, childrenResult, leave + 1, step, maxX, maxY)) {
                childrenResult.add((long) x << 32 | y);
                if (leave == 0) {
                    pathFlashback(childrenResult, result);
                }
                return true;
            }
        } else {
            if (pathData2(x, y + 1, x1, y1, callback, childrenResult, leave + 1, step + 1, maxX, maxY) || pathData2(x + 1, y, x1, y1, callback, childrenResult, leave + 1, step + 1, maxX, maxY)) {
                childrenResult.add((long) x << 32 | y);
                if (leave == 0) {
                    pathFlashback(childrenResult, result);
                }
                return true;
            }
        }
        return false;
    }

    private static void pathFlashback(List<Long> localPath, List<Long> resultPath) {
        for (int i = localPath.size() - 1; i >= 0; i--) {
            resultPath.add(localPath.get(i));
        }
    }


    private static boolean pointGo(int x, int y, Callback callback, List<Long> newPoint, List<Long> newPointPath, int leave, long from) {
        int maxX = callback.getOldListSize();
        int maxY = callback.getNewListSize();
        if (x == maxX && y == maxY) {
            addNewPoint(newPoint, point2Long(x, y), newPointPath, from);
            return true;
        }
        boolean result = false;
        if (x < maxX && y < maxY && callback.areItemsTheSame(x, y)) {
            result = pointGo(x + 1, y + 1, callback, newPoint, newPointPath, leave, from);
        } else if (leave == 1) {
            addNewPoint(newPoint, point2Long(x, y), newPointPath, from);
            return false;
        } else {
            // 向下
            if (y < maxY) {
                result = pointGo(x, y + 1, callback, newPoint, newPointPath, leave + 1, from);
            }
            // 向右
            if (x < maxX) {
                result |= pointGo(x + 1, y, callback, newPoint, newPointPath, leave + 1, from);
            }
        }
        return result;
    }

    private static void addNewPoint(List<Long> newPoint, Long vl, List<Long> newPointPath, long from) {
        // 优化排重
        for (int i = 0; i < newPoint.size(); i++) {
            if (newPoint.get(i).equals(vl) && newPointPath.get(i).equals(from)) {
                return;
            }
        }
        newPoint.add(vl);
        newPointPath.add(from);
    }

    private static void toFlashBack(Map<Integer, long[]> kv, Map<Integer, long[]> kvPath, int maxD, int targetX, int targetY, List<int[]> resultPath) {
        if (maxD < 1) {
            resultPath.add(new int[]{0, 0});
            return;
        }
        long[] lastPoint = kv.get(maxD);
        long[] lastPointPath = kvPath.get(maxD);
        int[] bestPoint = new int[2];
        double max = -1;
        for (int i = 0; i < lastPoint.length; i++) {
            int[] point = longVl2Point(lastPoint[i]);
            int x = point[0];
            int y = point[1];
            if (x == targetX && y == targetY) {
                point = longVl2Point(lastPointPath[i]);
                int fromX = point[0];
                int fromY = point[1];
                double a = targetX - fromX;
                double m = a == 0 ? 0 : Math.atan(a / (targetY - fromY));
                if (m > max) {
                    max = m;
                    bestPoint = new int[]{fromX, fromY};
                }
            }
        }
        toFlashBack(kv, kvPath, maxD - 1, bestPoint[0], bestPoint[1], resultPath);
        resultPath.add(new int[]{targetX, targetY});
    }

    private static int[] longVl2Point(long vl) {
        return new int[]{(int) (vl >>> 32), (int) (vl)};
    }

    private static long point2Long(int x, int y) {
        return ((long) x << 32) | y;
    }

    private static long[] listLong2Array(List<Long> list) {
        long[] result = new long[list.size()];
        int count = 0;
        for (long l : list) {
            result[count++] = l;
        }
        return result;
    }


    public static <C, R> R computeSource(ResultCallback<C, R> callback, byte[] path) {
        byte version = path[0];
        byte splitLen = path[1];
        System.out.println("version=" + version);
        System.out.println("dataLength=" + path.length);
        List<C> source = new LinkedList<>();
        int index = 2;
        int targetIndex = 0;
        while (index < path.length) {
            byte type = (byte) (path[index] >>> 6 & 0x03);
            if (type == 0) {
                byte len = path[index++];
                byte[] bytes = new byte[len];
                System.arraycopy(path, index, bytes, 0, len);
                C data = callback.bytes2Content(bytes);
                System.out.println("删除：字符=" + data);
                index += len;
                source.add(data);
            } else {
                int count = (path[index] & 0x3f) << 24 | path[index + 1] << 16 | path[index + 2] << 8 | (path[index + 3] & 0xff);
                System.out.println((type == 1 ? "添加" : "不变") + "：长度=" + count);
                index += 4;
                // 不变时
                if (type != 1) {
                    for (int i = 0; i < count; i++) {
                        source.add(callback.positionContent(targetIndex + i, splitLen));
                    }
                }
                targetIndex += count;
            }
        }
        return callback.list2Result(source);
    }


    public interface Callback {
        int getOldListSize();

        int getNewListSize();

        boolean areItemsTheSame(int oldItemPosition, int newItemPosition);

        byte[] sourceContent(int index);
    }

    public interface ResultCallback<C, R> {
        C bytes2Content(byte[] data);

        C positionContent(int index, byte len);

        R list2Result(List<C> children);
    }


//    ------------------------------------------------------------------------------------------------------------------------------------
//    ----------------------------------------------测试部分-------------------------------------------------------------------------------
//    ------------------------------------------------------------------------------------------------------------------------------------


    public static void write(byte[] data, String fileName, boolean old) throws IOException {
        String path = "/Users/keray/Desktop/diff/" + (old ? "old/" : "new/") + fileName;
        File file = new File(path);
        if (!file.exists() && !file.createNewFile()) {
            throw new RuntimeException();
        }
        FileOutputStream outputStream = new FileOutputStream(path);
        outputStream.write(data);
        outputStream.flush();
        outputStream.close();
    }

    public static byte[] read(String fileName, boolean old) throws IOException {
        String path = "/Users/keray/Desktop/diff/" + (old ? "old/" : "new/") + fileName;
        FileInputStream inputStream = new FileInputStream(path);
        byte[] data = new byte[inputStream.available()];
        inputStream.read(data);
        return data;
    }

    public static int computePath(String source, String target, String fileName) throws IOException {
        int l = (source.length() / 1024 + 1);
        byte len;
        if (l > 0x3f) {
            len = (byte) 0x3f;
        } else {
            len = (byte) l;
        }
        int LEN = len;
        System.out.println("len:" + LEN);
        Callback callback = new Callback() {
            @Override
            public int getOldListSize() {
                return (int) Math.floor(source.length() / LEN);
            }

            @Override
            public int getNewListSize() {
                return (int) Math.floor(target.length() / LEN);
            }

            @Override
            public boolean areItemsTheSame(int oldItemPosition, int newItemPosition) {
                int oldIndex = oldItemPosition * LEN;
                int newIndex = newItemPosition * LEN;
                return source.substring(oldIndex, Math.min(oldIndex + LEN, source.length())).equals(
                        target.substring(newIndex, Math.min(newIndex + LEN, target.length()))
                );
            }

            @Override
            public byte[] sourceContent(int index) {
                int newIndex = index * LEN;
                return source.substring(newIndex, Math.min(newIndex + LEN, source.length())).getBytes();
            }
        };
        long time = System.currentTimeMillis();
        byte[] result = StringDiff.myers(callback, len);
        time = System.currentTimeMillis() - time;
        write(source.getBytes(StandardCharsets.UTF_8), fileName, true);
        write(result, fileName, false);
        return (int) time;
    }


    public static void main(String[] args) {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "5");
        int count = 16 * 1024;
        List<String> fileName = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            fileName.add("source-" + i + ".txt");
        }
        System.out.println(fileName.stream().parallel()
                .map(f -> {
                    int xx = 1024 * 64;
                    int yx = 10;
//                    String source = "ABCABBA";
//                    String target1 = "CBABAC";

                    String source = RandomUtil.randomString(xx);
                    char[] target = source.toCharArray();
                    int k = RandomUtil.randomInt(yx);
                    int kc = Math.min(xx / yx, 100);
                    System.out.println("随机更新组：" + k);
                    for (int i = 0; i < k; i++) {
                        String r = RandomUtil.randomString(kc);
                        for (int index = 0; index < r.length(); index++) {
                            target[i * kc + index] = r.charAt(index);
                        }
                    }
                    String target1 = new String(target);

                    try {
                        int time = computePath(source, target1, f);
                        System.out.println("耗时：" + time);
                        byte[] oldByte = read(f, true);
                        byte[] newByte = read(f, false);
                        boolean r = new String(oldByte).equals(
                                computeSource(new ResultCallback<String, String>() {
                                    @Override
                                    public String bytes2Content(byte[] data) {
                                        return new String(data);
                                    }

                                    @Override
                                    public String positionContent(int index, byte len) {
                                        int newIndex = index * len;
                                        return target1.substring(newIndex, Math.min(newIndex + len, target1.length()));
                                    }

                                    @Override
                                    public String list2Result(List<String> children) {
                                        return String.join("", children);
                                    }
                                }, newByte)
                        );
                        if (!r) {
                            System.err.println("==========错误==========");
                            throw new RuntimeException();
                        }
                        return time;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return 0;
                }).mapToInt(e -> e).sum());


    }
}
