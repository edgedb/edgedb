Semantix.dv.DocPanelDragZone = Ext.extend(Semantix.ext.DragZone, {
    getDragData: function(e) {
        var source = e.getTarget('.semantix-draggable', 2);

        if (source) {
            topic_id = Ext.fly(source).getAttributeNS('semantix', 'entity-id');

            d = source.cloneNode(true);
            d.id = Ext.id();
            return this.dragData = {
                sourceEl: source,
                repairXY: Ext.fly(source).getXY(),
                ddel: d,

                node: {id: topic_id}
            }
        }
    },

    getRepairXY: function() {
        return this.dragData.repairXY;
    }
});

Semantix.dv.DocPanel = function(config) {
    Semantix.dv.DocPanel.superclass.constructor.call(this, Ext.apply({
        title:Ext.id(),
        autoScroll: true,
        border: false,
        closable: true
    }, config));

    this.topics_dd_group = 'semantix-dv-dd-documents-tree';

    this.on('render', this.initDocPanel, this);
};

Ext.extend(Semantix.dv.DocPanel, Ext.Panel, {
    initDocPanel : function() {
        this.getUpdater().on('update', this.initTopicLayout, this);

        this.dragZone = new Semantix.dv.DocPanelDragZone(this.body, {
            ddGroup: this.topics_dd_group,
        });
    },

    loadPage : function(id) {
        this.getUpdater().update({
            url: "/get_topic",
            params: {
                   entity_id: id
            }
        });
    },

    initTopicLayout : function(body) {
        this.body.on({
            'mousedown': function(e, t){
                t.target = '_blank';
            },
            'click': function(e, t){
                e.stopEvent();
                topic_id = Ext.fly(t).getAttributeNS('semantix', 'entity-id');
                Semantix.Bubbling.fire('semantix.dv.topics.selected', topic_id);
            },
            delegate:'a'
        });
    }
});
