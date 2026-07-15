package demo.api.domain;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface AggregateRepository extends JpaRepository<AggregateEntity, Long> {

    List<AggregateEntity> findByComponentIdOrderByWindowStartDesc(String componentId, Pageable pageable);

    @Query("SELECT DISTINCT a.componentId FROM AggregateEntity a ORDER BY a.componentId")
    List<String> findDistinctComponentIds();
}
