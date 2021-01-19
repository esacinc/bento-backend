package gov.nih.nci.bento.model;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class GraphQlQuery {
    private String query;
    private String variables;
}
