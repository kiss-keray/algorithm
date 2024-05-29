package com.jingxun;

import cn.hutool.core.util.HexUtil;
import cn.hutool.crypto.ECKeyUtil;
import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.asymmetric.SM2;

public class Test {
    public static void main(String[] args) throws Exception {
        String content = "我是Hanley.";
        var puk = "041884344ef60e621ed3b14cdbe3bf693d568fc61c17e70e029c8009b462b64d29c31615e6eb6d80152f4e42da94f79e01296bf90553d8ddd2d5cf76bc617784c8";
        var prk = "300c90cc7c1417b85c635db26884541733644c0912705484efb8d6b587529c8a";
        final SM2 sm2 = new SM2(ECKeyUtil.decodePrivateKeyParams(SecureUtil.decode(prk)), ECKeyUtil.decodePublicKeyParams(SecureUtil.decode(puk)));
        String sign = sm2.signHex(HexUtil.encodeHexStr(content));

        boolean verify = sm2.verifyHex(HexUtil.encodeHexStr(content), sign);

        System.out.println(verify);

    }
}
