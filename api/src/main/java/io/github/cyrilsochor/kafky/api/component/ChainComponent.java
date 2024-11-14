package io.github.cyrilsochor.kafky.api.component;

public interface ChainComponent<C extends Component> extends Component {

    void setChainNext(C nextChainComponent);

    C getChainNext();

    default int getPriority() {
        return 0;
    }

    @Override
    default void shutdownHook() {
        final C chainNext = getChainNext();
        if (chainNext != null) {
            chainNext.shutdownHook();
        }
    }

}
