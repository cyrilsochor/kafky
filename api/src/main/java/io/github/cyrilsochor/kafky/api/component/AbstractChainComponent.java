package io.github.cyrilsochor.kafky.api.component;

public abstract class AbstractChainComponent<C extends ChainComponent<C>> implements ChainComponent<C> {

    // nullable
    protected C chainNext;

    @Override
    public void setChainNext(final C chainNext) {
        this.chainNext = chainNext;
    }

    @Override
    public C getChainNext() {
        return chainNext;
    }

    @Override
    public void init() throws Exception {
        if (chainNext != null) {
            chainNext.init();
        }
    }

    @Override
    public void close() throws Exception {
        if (chainNext != null) {
            chainNext.close();
        }
    }

}
