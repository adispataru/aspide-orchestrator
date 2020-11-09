package alien4cloud.aspide;

import javax.validation.constraints.NotNull;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import alien4cloud.ui.form.annotation.FormLabel;
import alien4cloud.ui.form.annotation.FormProperties;
import alien4cloud.ui.form.annotation.FormPropertyConstraint;
import alien4cloud.ui.form.annotation.FormPropertyDefinition;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@FormProperties({ "url", "user", "password", "location", "dcePath", "providers" })
public class Configuration {

    @FormLabel("ASPIDE Orchestrator URL")
    @FormPropertyConstraint(pattern = "http\\:.+(?:\\d+)")
    @NotNull
    private String url;

    @FormLabel("SLURM User")
    private String user;

    @FormLabel("SLURM Password")
    @FormPropertyDefinition(type = "string", isPassword = true)
    private String password;

    @FormLabel("DCEx Runtime Path")
    private String dcePath;

    @FormLabel("Default ASPIDE Location")
    private String location;


    @FormLabel("Tosca Metadata Providers")
    private List<String> providers;
}
