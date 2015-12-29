package com.egopulse.querydsl.rethinkdb;

import com.querydsl.apt.AbstractQuerydslProcessor;
import com.querydsl.apt.Configuration;
import com.querydsl.apt.DefaultConfiguration;
import com.querydsl.core.annotations.*;

import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import java.util.Collections;

@SupportedAnnotationTypes({"com.querydsl.core.annotations.*"})
public class RethinkDBAnnotationProcessor extends AbstractQuerydslProcessor {
    @Override
    protected Configuration createConfiguration(RoundEnvironment roundEnvironment) {
        return new DefaultConfiguration(
                roundEnvironment,
                this.processingEnv.getOptions(),
                Collections.<String>emptySet(),
                QueryEntities.class,
                QueryEntity.class,
                QuerySupertype.class,
                QueryEmbeddable.class,
                QueryEmbedded.class,
                QueryExclude.class);
    }
}
