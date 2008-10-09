Ext.ux.TabPanel = Ext.extend(Ext.TabPanel, {
    initTab : function(item, index){
        Ext.ux.TabPanel.superclass.initTab.apply(this, arguments);
        item._tab_button = Ext.get(this.id + this.idDelimiter + item.getItemId());
    }
});
