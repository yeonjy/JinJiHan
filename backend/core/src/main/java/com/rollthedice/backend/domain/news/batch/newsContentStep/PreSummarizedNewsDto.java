package com.rollthedice.backend.domain.news.batch.newsContentStep;

import lombok.*;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PreSummarizedNewsDto {
    private Long id;
    private String title;
    private String content;
    private String postDate;
}
