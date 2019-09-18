import * as React from "react";
import TextField from "@material-ui/core/TextField";
import Checkbox from "@material-ui/core/Checkbox";
import IconButton from "@material-ui/core/IconButton";
import DeleteIcon from "@material-ui/icons/Delete";
import {withStyles} from "@material-ui/core";
import todoItemStyles from "./todoItemStyles";

function TodoItem({classes, name, isDone, onDeleteItem, onRenameItem, onToggleItemStatus}) {
  const onStatusChange = () => {
    onToggleItemStatus(name);
  };

  const onDelete = () => onDeleteItem(name);

  const onNameChange = event => {
    onRenameItem(name, Date.now() + ';' + event.target.value);
  };

  return (
    <TextField
      value={name.split(';')[1]}
      fullWidth
      onChange={event => onNameChange(event)}
      className={isDone ? classes.textField : null}
      InputProps={{
        classes: {
          root: classes.itemInput,
        },
        startAdornment: (
          <Checkbox
            checked={isDone}
            className={classes.checkbox}
            onChange={onStatusChange}
          />
        ),
        endAdornment: (
          <IconButton
            className={classes.deleteIconButton}
            onClick={onDelete}
          >
            <DeleteIcon/>
          </IconButton>
        )
      }}
    />
  );
}

export default withStyles(todoItemStyles)(TodoItem);
