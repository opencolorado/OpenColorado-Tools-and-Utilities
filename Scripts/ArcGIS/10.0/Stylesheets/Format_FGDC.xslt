<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" omit-xml-declaration="yes" />

    <!-- process the metadata using the templates below -->
    <xsl:template match="/">
        <xsl:apply-templates select="node() | @*" />
    </xsl:template>

    <!-- copy all metadata conent -->
    <xsl:template match="node() | @*" priority="0">
        <xsl:copy>
            <xsl:apply-templates select="node() | @*" />
        </xsl:copy>
    </xsl:template>

    <!-- all metadata XSLT stylesheets used to update metadata should be identical to this example up to this point -->
    <!-- add the templates you'll use to update the metadata below -->

    <!-- Remove the point of contact info -->
    <xsl:template match="//ptcontac">
    </xsl:template>

    <!-- Remove the geoprocessing lineage -->
    <xsl:template match="//lineage">
    </xsl:template>

    <!-- Remove the metadata contact info -->
    <xsl:template match="//metc">
    </xsl:template>
    
    <!-- Remove any information about internal servers -->
    <xsl:template match="//onlink">
    </xsl:template>
    <xsl:template match="//computer">
    </xsl:template>        
    
</xsl:stylesheet>