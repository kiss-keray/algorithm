package com.jingxun;

import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class Demo2 {
    public static void main(String[] args) throws Exception {
        XSSFWorkbook xwb = new XSSFWorkbook(new File("上海交通大学机构词典.xlsx"));
        var sheet = xwb.getSheetAt(0);
        String last = "";
        List<String> result = new LinkedList<>();
        List<String> lastResult = new LinkedList<>();
//        XSSFWorkbook x = new XSSFWorkbook();
//        var s = x.createSheet();
//        var r = s.createRow(0);
//        r.createCell(0).setCellValue("文本1");
//        r.createCell(1).setCellValue("文本2");
//        r.createCell(2).setCellValue("相似度标签");
        var stream = new FileOutputStream("data11.txt");
        for (var i = 1; i <= sheet.getLastRowNum(); i++) {
            var row = sheet.getRow(i);
            try {

                // 一级机构
                var oneCell = row.getCell(0).getStringCellValue();
                // 二级机构
                var twoCell = row.getCell(1) != null && row.getCell(1).getCellType() == CellType.STRING ? row.getCell(1).getStringCellValue() : "";
                // 三级机构
                var threeCell = row.getCell(2) != null && row.getCell(2).getCellType() == CellType.STRING ? row.getCell(1).getStringCellValue() : "";
                // 四级机构
                var fourCell = row.getCell(3) != null && row.getCell(3).getCellType() == CellType.STRING ? row.getCell(1).getStringCellValue() : "";
                // 语言
                var lang = row.getCell(4).getStringCellValue();
                // 别名
                var name = row.getCell(5).getStringCellValue();
                var key = String.format("%s%s%s%s", oneCell, twoCell, threeCell, fourCell);
                if ("英文".equals(lang)) continue;
                if (key.equals(last)) {
                    result.add(name);
                } else {
                    // 写入文件
                    write(result, stream);
                    write1(result, lastResult, stream);
                    lastResult.clear();
                    lastResult.addAll(result);
                    result.clear();
                }
                last = key;
            } catch (Exception e) {
                System.out.println(11);
            }
        }
//        x.write(new FileOutputStream("data.xlsx"));
//        x.close();
        stream.flush();
        stream.close();
        xwb.close();
    }

    private static void write(List<String> data, OutputStream stream) throws IOException {
        for (var k : data) {
            for (var k1 : data) {
                if (k.equals(k1)) continue;
                stream.write(String.format("%s\t%s\t1\n", k, k1).getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static void write1(List<String> data, List<String> data1, OutputStream stream) throws IOException {
        for (var k : data) {
            for (var k1 : data1) {
                if (k.equals(k1)) continue;
                stream.write(String.format("%s\t%s\t0\n", k, k1).getBytes(StandardCharsets.UTF_8));
            }
        }
    }

}
