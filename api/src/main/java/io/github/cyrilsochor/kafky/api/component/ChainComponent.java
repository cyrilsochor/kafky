package io.github.cyrilsochor.kafky.api.component;

public interface ChainComponent<C> extends Component {

    void setChainNext(C nextChainComponent);

    C getChainNext();

    default int getPriority() {
        return 0;
    }

}
