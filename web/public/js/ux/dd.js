Semantix.ext.DragZone = Ext.extend(Ext.dd.DragZone, {
    onInitDrag : function(){
        Semantix.ext.DragZone.superclass.onInitDrag.apply(this, arguments);
        Semantix.Bubbling.fire('semantix.dv.highlight-ddzone.on', this.ddGroup);
    },

    onEndDrag : function() {
        Semantix.ext.DragZone.superclass.onEndDrag.apply(this, arguments);
        Semantix.Bubbling.fire('semantix.dv.highlight-ddzone.off', this.ddGroup);
    }
});

Semantix.ext.TreeDragZone = Ext.extend(Ext.tree.TreeDragZone, {
    onInitDrag : function(){
        Semantix.ext.TreeDragZone.superclass.onInitDrag.apply(this, arguments);
        Semantix.Bubbling.fire('semantix.dv.highlight-ddzone.on', this.ddGroup);
    },

    onEndDrag : function() {
        Semantix.ext.TreeDragZone.superclass.onEndDrag.apply(this, arguments);
        Semantix.Bubbling.fire('semantix.dv.highlight-ddzone.off', this.ddGroup);
    }
});

