package ips_project.ips.model;

import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;
import org.neo4j.ogm.annotation.RelationshipEntity;
import org.neo4j.ogm.annotation.StartNode;
import org.springframework.data.annotation.Id;

@RelationshipEntity(type = "PENDING")
public class Rating {

    @Id
    private long userId;
    @StartNode
    private long movieId;
    @Property
    private int tag;
    @Property
    private int timestamp;

}
