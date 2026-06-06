import powerbi from "powerbi-visuals-api";
import VisualConstructorOptions = powerbi.extensibility.visual.VisualConstructorOptions;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;
import IVisual = powerbi.extensibility.visual.IVisual;

import * as React from "react";
import { createRoot } from "react-dom/client";
import { ChatVisual } from "./ChatVisual";

// ── Power BI Visual Host Wrapper ─────────────────────────────────────────────
// This class is the entry point Power BI calls to create/update/destroy the visual.

export class Visual implements IVisual {
    private root: any;

    // Called once when the visual is first initialized in Power BI
    constructor(options: VisualConstructorOptions) {
        // Create a React root bound to the Power BI DOM element
        this.root = createRoot(options.element);

        // Render the main React component
        this.root.render(React.createElement(ChatVisual));
    }

    // Called whenever Power BI sends new data or viewport updates
    public update(options: VisualUpdateOptions) {
     
    }

    // Called when the visual is removed from the report or refreshed
    public destroy() {
        // Clean up React tree to avoid memory leaks in Power BI sandbox
        this.root.unmount();
    }
}