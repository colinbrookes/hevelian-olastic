package com.hevelian.olastic.core.edm;

import java.util.Iterator;

import org.apache.olingo.commons.api.edm.EdmBindingTarget;
import org.apache.olingo.commons.api.edm.EdmEntityContainer;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.edm.EdmNavigationPropertyBinding;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.core.edm.EdmEntitySetImpl;
import org.apache.olingo.commons.core.edm.Target;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEntitySet;

/**
 * Custom implementation of {@link EdmEntityType}.
 * 
 * @author rdidyk
 */
public class ElasticEdmEntitySet extends EdmEntitySetImpl {

    private ElasticCsdlEntitySet csdlEntitySet;
    private ElasticEdmProvider provider;

    public ElasticEdmEntitySet(ElasticEdmProvider provider, EdmEntityContainer container,
            ElasticCsdlEntitySet entitySet) {
        super(provider, container, entitySet);
        this.provider = provider;
        this.csdlEntitySet = entitySet;
    }

    /**
     * Get's index name in Elasticsearch.
     * 
     * @return index name
     */
    public String getEIndex() {
        return csdlEntitySet.getEIndex();
    }

    /**
     * Get's type name in Elasticsearch.
     * 
     * @return type name
     */
    public String getEType() {
        return csdlEntitySet.getEType();
    }

    @Override
    public ElasticEdmEntityType getEntityType() {
        EdmEntityType entityType = provider.getEntityType(new FullQualifiedName(
                csdlEntitySet.getTypeFQN().getNamespace(), csdlEntitySet.getEType()));
        return entityType != null ? (ElasticEdmEntityType) entityType
                : (ElasticEdmEntityType) provider.getEntityType(csdlEntitySet.getTypeFQN());
    }

    /**
     * Sets index to entity set.
     * 
     * @param eIndex
     */
    public void setEIndex(String eIndex) {
        csdlEntitySet.setEIndex(eIndex);
    }

    /**
     * Sets type to entity set.
     * 
     * @param eType
     */
    public void setEType(String eType) {
        csdlEntitySet.setEType(eType);
    }

    /**
     * Override because of if child entity type has name which starts with as
     * parent entity type name, then wrong entity set is returning.
     */
    @Override
    public EdmBindingTarget getRelatedBindingTarget(final String path) {
        if (path == null) {
            return null;
        }
        EdmBindingTarget bindingTarget = null;
        boolean found = false;
        for (Iterator<EdmNavigationPropertyBinding> itor = getNavigationPropertyBindings()
                .iterator(); itor.hasNext() && !found;) {
            EdmNavigationPropertyBinding binding = itor.next();
            if (binding.getPath() == null || binding.getTarget() == null) {
                throw new EdmException(
                        "Path or Target in navigation property binding must not be null!");
            }
            // Replace 'startsWith' to 'equals'
            if (path.equals(binding.getPath())) {
                Target target = new Target(binding.getTarget(), getEntityContainer());
                EdmEntityContainer entityContainer = edm
                        .getEntityContainer(target.getEntityContainer());
                if (entityContainer == null) {
                    throw new EdmException("Cannot find entity container with name: "
                            + target.getEntityContainer());
                }
                try {
                    bindingTarget = entityContainer.getEntitySet(target.getTargetName());
                    if (bindingTarget == null) {
                        throw new EdmException("Cannot find EntitySet " + target.getTargetName());
                    }
                } catch (EdmException e) {
                    bindingTarget = entityContainer.getSingleton(target.getTargetName());
                    if (bindingTarget == null) {
                        throw new EdmException("Cannot find Singleton " + target.getTargetName(),
                                e);
                    }
                } finally {
                    found = bindingTarget != null;
                }
            }
        }
        return bindingTarget;
    }

}
