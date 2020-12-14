package com.ychenchen.dto;

/**
 * @author alexis.yang
 * @since 2020/12/14 1:05 PM
 */
public class WordAndCount {
    private String word;

    private int count;

    public WordAndCount() {
    }

    public WordAndCount(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordAndCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
