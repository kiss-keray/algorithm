package com.jingxun;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class Demo {
    public static void main(String[] args) throws Exception {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "500");

        var pool = new ThreadPoolExecutor(20, 20, 10, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(1000000), r -> {
            var t = new Thread(r);
            t.setName("work");
            return t;
        });
        var demo = new Demo();
//        XSSFWorkbook xwb = new XSSFWorkbook(new File("上海交通大学机构词典.xlsx"));
//        var sheet = xwb.getSheetAt(0);
//        var semaphore = new Semaphore(20);
//        for (var i = 1; i <= sheet.getLastRowNum(); i++) {
//            if (semaphore.tryAcquire()) {
//                var row = sheet.getRow(i);
//                pool.execute(() -> {
//                    // 一级机构
//                    var oneCell = row.getCell(0).getStringCellValue();
//                    // 二级机构
//                    var twoCell = row.getCell(1).getStringCellValue();
//                    // 三级机构
//                    var threeCell = row.getCell(2).getStringCellValue();
//                    // 四级机构
//                    var fourCell = row.getCell(3).getStringCellValue();
//                    // 生成输入数据
//                    demo.inputLine(String.format("%s-%s-%s-%s", oneCell, twoCell, threeCell, fourCell));
//                    semaphore.release();
//                });
//            } else {
//                i--;
//            }
//        }
//        xwb.close();
        demo.inputLine("上海交通太学-动力工程学院-工业工程管理系");
        demo.inputLine("交通大学-机械动力工程学院-工业工程系");
        demo.inputLine("上海交通大学1-动力工程学院-工业工程与管理系");
        demo.inputLine("交大-机械与动力工程学院-工业工程系");
        demo.inputLine("交大-计算机-软件处");
        demo.inputLine("交大-计算机1-软件处");
        demo.inputLine("交大-计算机1-软件处1");
        demo.printTree();
    }

    public class TreeNodeData {
        /**
         * 数据
         */
        String data;
        /**
         * 挂载点
         */
        TreeNode point;
    }


    public class TreeNode {
        /**
         * 当前节点数据
         */
        LinkedList<TreeNodeData> data = new LinkedList<>();

        /**
         * 上级
         */
        TreeNode parent;

        /**
         * 下级
         */
        LinkedList<TreeNode> children = new LinkedList<>();

    }

    private final TreeNode root = new TreeNode();

    public void inputLine(String lineData) {
        String[] data = lineData.split("-");
        TreeNode parent = root;
        for (int i = 0; i < data.length; i++) {
            var name = data[i];
            parent = processWord(i + 1, name, parent);
        }
    }


    private TreeNode processWord(int level, String str, TreeNode parent) {
        if (str.isEmpty()) return null;
        // 找到同层级下的所有节点
        List<TreeNode> levelData = getLevelData(level, root);
        // 实例化当前数据对象
        TreeNodeData treeNodeData = new TreeNodeData();
        treeNodeData.data = str;
        // 遍历当前层节点
        levelData.forEach(node -> {
            // 判断节点的父节点不等于当前数据的父节点
            if (node.parent != parent) {
                // 如果节点存在数据点与当前数据点的相似度大于60 则将当前数据点添加到当前节点
                if (node.data.stream().anyMatch(nodeData -> wordSimilarity(nodeData.data, str) > 60)) {
                    // 回归分析上层
                    regressionAnalysis(parent, node.parent);
                    node.data.add(treeNodeData);
                }
            }
        });
        var maxSimilarity = 0;
        TreeNode addNode = null;
        // 遍历兄弟节点找合适的落脚点
        for (var pNode : parent.children) {
            var havaAdd = false;
            var equals = false;
            for (var pNodeData : pNode.data) {
                var similarity = wordSimilarity(pNodeData.data, str);
                equals = equals || pNodeData.data.equals(str);
                if (similarity > 60) {
                    if (maxSimilarity < similarity) {
                        maxSimilarity = similarity;
                        addNode = pNode;
                    }
                    havaAdd = true;
                }
            }
            if (havaAdd && !equals) {
                pNode.data.add(treeNodeData);
            }
        }
        // 如果当前兄弟节点没找到合适的落脚点
        if (addNode == null) {
            // 直接在父节点给自己造一个落脚点
            var node = new TreeNode();
            node.data.add(treeNodeData);
            node.parent = parent;
            addNode = node;
            parent.children.add(node);
        }
        treeNodeData.point = addNode;
        return treeNodeData.point;
    }

    /**
     * 回归分析当前节点之上的同层节点的之间的关系
     *
     * @param node
     * @param node1
     */
    private void regressionAnalysis(TreeNode node, TreeNode node1) {

    }

    private List<TreeNode> getLevelData(int level, TreeNode node) {
        if (level == 1) return node.children;
        return node.children.stream().flatMap(child -> getLevelData(level - 1, child).stream()).collect(Collectors.toList());
    }

    /**
     * 比较两个字符串的相似度
     *
     * @param wordA 字符串A
     * @param wordB 字符串B
     * @return 相似度 大于60就认为可以聚合
     */
    private int wordSimilarity(String wordA, String wordB) {
        if (wordA.equals(wordB)) return 100;
        List<String> list = Arrays.asList("上海交通太学", "交通大学", "上海交通大学1", "交大");
        if (list.contains(wordA) && list.contains(wordB)) {
            return 80;
        }
        list = Arrays.asList("动力工程学院", "机械动力工程学院", "动力工程学院", "机械与动力工程学院");
        if (list.contains(wordA) && list.contains(wordB)) {
            return 80;
        }
        list = Arrays.asList("工业工程管理系", "工业工程与管理系", "工业工程系");
        if (list.contains(wordA) && list.contains(wordB)) {
            return 80;
        }
        list = Arrays.asList("软件处1", "软件处");
        if (list.contains(wordA) && list.contains(wordB)) {
            return 80;
        }
        return 10;
    }

    private void printTree() {
        Stack<TreeNode> stack = new Stack<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            var now = stack.pop();
            if (now.children.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (var node = now; node != root; node = node.parent) {
                    sb.insert(0, String.format("->[%s]", node.data.stream().map(v -> v.data).collect(Collectors.joining(","))));
                }
                System.out.println(sb.substring(2));
                continue;
            }
            for (var child : now.children) {
                stack.push(child);
            }
        }
    }
}
