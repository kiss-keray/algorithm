package com.jingxun;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.xm.Similarity;

import java.io.File;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Demo1 {
    public static void main(String[] args) throws Exception {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "500");

        var demo = new Demo1();
        XSSFWorkbook xwb = new XSSFWorkbook(new File("上海交通大学机构词典.xlsx"));
        var sheet = xwb.getSheetAt(0);
        for (var i = 1; i <= sheet.getLastRowNum(); i++) {
            var row = sheet.getRow(i);
            // 曾用名
            var fiveCell = row.getCell(5).getStringCellValue();
            // 生成输入数据
            demo.inputLine(fiveCell);
            System.out.print(i + " ");
        }
        demo.printData();
        xwb.close();

    }

    private final List<List<String>> data = new LinkedList<>();

    public void inputLine(String word) {
        var haveInsert = false;
        for (var list : data) {
            haveInsert = haveInsert | wordInsert(word, list);
        }
        if (!haveInsert) {
            data.add(new LinkedList<>() {{
                add(word);
            }});
        }
    }

    private boolean wordInsert(String word, List<String> block) {
        final AtomicInteger max = new AtomicInteger(0);
        int allScore = block.parallelStream().mapToInt(v -> {
            var val = wordSimilarity(v, word);
            if (val > max.get()) {
                synchronized (max) {
                    if (val > max.get()) {
                        max.set(val);
                    }
                }
            }
            return val;
        }).sum();

        if (max.get() >= 80 && allScore / block.size() > 50.0) {
            block.add(word);
            return true;
        }
        return false;
    }


    /**
     * 比较两个字符串的相似度
     *
     * @param wordA 字符串A
     * @param wordB 字符串B
     * @return 相似度 大于60就认为可以聚合
     */
    private int wordSimilarity(String wordA, String wordB) {
        if (isEn(wordA) || isEn(wordB)) {
            return (int) (Similarity.pinyinSimilarity(wordA, wordB) * 100);
        }
        return (int) (Similarity.conceptSimilarity(wordA, wordB) * 100);
    }

    /**
     * 判断字符串是否是英语
     *
     * @param word 字符串
     * @return
     */
    private boolean isEn(String word) {
        return word.matches("^[\\w| ]+$");
    }

    private void printData() throws Exception {
        File file = new File("out.text");
        if (!file.exists()) {
            file.createNewFile();
        }
        FileWriter writer = new FileWriter(file);
        for (var list : data) {
            writer.write("【" + String.join(",", list) + "】");
            writer.write(System.lineSeparator());
        }
        writer.flush();
        writer.close();
    }
}
