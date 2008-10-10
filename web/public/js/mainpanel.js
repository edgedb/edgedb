Semantix.dv.MainPanel = function(config) {
    Semantix.dv.MainPanel.superclass.constructor.call(this, Ext.apply({
        enableTabScroll: true
    }, config));

    this._tabs = {};

    Semantix.Bubbling.on('semantix.dv.topics.selected', this.onTopicSelected, this);

    this.on('remove', this.onCloseTopic, this);
};

Ext.extend(Semantix.dv.MainPanel, Ext.ux.TabPanel, {
    onTopicSelected : function(topic_id) {
        if (this._tabs.hasOwnProperty(topic_id) && this._tabs[topic_id]) {
            this.setActiveTab(this._tabs[topic_id]);
            return;
        }

        var t = this._tabs[topic_id] = this.add(new Semantix.dv.DocPanel({
            topic_id: topic_id
        }));

        this.setActiveTab(t);
        t.loadPage(topic_id);
    },

    onCloseTopic : function(self, tab) {
        id = tab.topic_id;

        if (this._tabs[id]) {
            tab.destroy();

            delete this._tabs[id];
            delete tab;
        }
    }
});
