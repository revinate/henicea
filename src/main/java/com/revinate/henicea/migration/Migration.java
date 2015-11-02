package com.revinate.henicea.migration;

import com.google.common.io.CharStreams;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.springframework.core.io.Resource;

import java.io.InputStreamReader;
import java.util.Optional;

@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class Migration {
    private final String name;
    private final String statement;

    static Optional<Migration> fromResource(Resource resource) {
        try {
            String statement = CharStreams.toString(new InputStreamReader(resource.getInputStream()));
            return Optional.of(new Migration(resource.getFilename(), statement));
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
