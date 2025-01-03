package org.example.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class WordCounter {

    @Value("${app.stop-words}.split(',').toLowerCase()")
    private Set<String> stopWords = new HashSet<>(Arrays.asList("the", "to", "a", "and", "is", "are", "or", "in", "at", "on", "me", "i", "we", "you", "he", "she", "it"));

    public List<String> splitText(String text) {
        return Arrays.asList(text.replaceAll("\\p{P}", "").toLowerCase().split("\\s"));
    }

    public Stream<WordFrequency> groupByWords(List<String> words) {
        return words.stream()
                .filter(this::isNotStopWord)
                .collect(Collectors.groupingBy(word -> word, Collectors.counting()))
                .entrySet()
                .stream().map(e -> new WordFrequency(e.getKey(), e.getValue()))
                .sorted(Comparator.comparing(WordFrequency::count).reversed());
    }

    private boolean isNotStopWord(String word) {
        return !stopWords.contains(word);
    }

}
